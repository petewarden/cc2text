#!/usr/bin/env ruby

require 'rubygems'
require 'aws-sdk'
require 'open3'
require 'json'
require 'hpricot'
require 'htmlentities'

# CHANGEME! - You'll need to put your own Amazon keys in here
s3=AWS::S3.new(
  :access_key_id=>'',
  :secret_access_key=>''
)

# Inline this so we don't have to copy a file while bootstrapping
# See http://www.archive.org/web/researcher/ArcFileFormat.php
# for information about the ARC format once it is decompressed
class ArcFileReader

  include Enumerable

  def initialize( input_stream )
    @handle=input_stream
  end

  def each
    return self.to_enum unless block_given?
    
    # The version block header looks something like this:
    #filedesc://1258794327404_0.arc.gz 0.0.0.0 20091121010527 text/plain 73
    version_block_header = @handle.readline.strip
    @filename, @ip_address, @filedate, @filetype, version_block_length_string = version_block_header.split(' ')
    version_block_length = version_block_length_string.to_i

    # The version block body looks like this:
    #1 0 CommonCrawl
    #URL IP-address Archive-date Content-type Archive-length
    version_block_string = @handle.read( version_block_length )
    version_block_lines = version_block_string.split("\n")
    @version, @reserved, @origin_code = version_block_lines[0].split(' ')
    @doc_header_columns = version_block_lines[1].split(' ')
      
    loop do
      begin
        # The url record looks like this:
        #http://mendozaopina.blogspot.com/ 209.85.229.191 20091103192002 text/html 298216
        url_record_string = @handle.readline.strip
        url_record_list = url_record_string.split(' ')
        url_record_headers = {}
        url_record_list.each_with_index do |value, index|
          header = @doc_header_columns[index]
          url_record_headers[header] = value
        end
        network_doc_length = url_record_headers['Archive-length'].to_i
        network_doc = @handle.read( network_doc_length )
        unless (byte=@handle.read(1))=="\n"
          raise ArgumentError, "#{self.class}: Corrupt ARCfile? Expected \\n as record terminator, got #{byte}"
        end
        yield [url_record_headers, network_doc]
      rescue EOFError
        break nil
      end
    end
  end

end

# Converts an HTML string into text using the Hpricot library
def html2text(html)

  web_doc = Hpricot(html)
  web_doc.search("//comment()").remove
  web_doc.search("script").remove
  web_doc.search("style").remove
  web_doc.search("noscript").remove
  web_doc.search("object").remove
  web_doc.search("embed").remove
  web_doc.search("head").remove

  result = ''
  begin
    web_doc.traverse_text do |e| 

      begin
        if e.content
          result += e.content+"\n"
        end
      rescue
        # ignore errors
      end
    end
  rescue
    # ignore errors
  end

  if result == ''
    # Use a simple regular-expression approach to remove all tags
    result = html.gsub(/<[^>]*>/, '')
  end

  coder = HTMLEntities.new
  result = coder.decode(result)

  result.gsub!(/\n[\r\n \t]*/, "\n")

  result
end

# Splits a raw network document dump (essentially the raw results of an HTTP GET) into
# a hash full of headers, and the content text
def doc2components(doc)
  headers_string, content = doc.split(/\r?\n\r?\n/, 2)
  headers_list = headers_string.split("\n")
  headers = {}
  status_line = headers_list.shift
  headers['x_commoncrawl_Status'] = status_line.strip
  headers_list.each do |header|
    name, value = header.strip.split(':', 2)
    headers[name] = value
  end
  [headers, content]
end

# This is the function that actually does the work on the crawled documents
def process_document(url_record_headers, network_doc)
  source_url = url_record_headers['URL']
  crawl_date = url_record_headers['Archive-date']
  content_type = url_record_headers['Content-type']
  if content_type != 'text/html' # Only process HTML pages
    return
  end
  # The source URL and the crawl date should make a decent unique key
  output_key = source_url+' '+crawl_date
  
  # Get the headers and content from the raw document string
  headers, html = doc2components(network_doc)  
  # Convert the HTML into rendered text
  text = html2text(html)
  # Mark this document as having been converted
  headers['Content-Type'] = 'text/plain'
  headers['x_commoncrawl_Converted-From'] = content_type
  # Output the results
  output_url_record_headers = url_record_headers
  output_url_record_headers['Content-type'] = 'text/plain'
  output_value = { 
    'url_record_headers' => output_url_record_headers,
    'network_doc' => {
      'headers' => headers,
      'content' => text,
    },
  }
  output_value_string = output_value.to_json
  puts "#{output_key}\t#{output_value_string}"
end

CHUNKSIZE=1024*1024

# All these warnings will end up in the EMR stderr logs.
warn "Starting up, using #{CHUNKSIZE/1024}KB chunks for download."

ARGF.each_line {|line|
  warn "Starting work on #{line.chomp}"
  # expect a line like this:
  # s3://commoncrawl-crawl-002/2010/09/24/9/1285380159663_9.arc.gz
  proto,unused,bucket_name,*rest=line.chomp.split File::SEPARATOR
  raise ArgumentError, "#{__FILE__}: Unknown S3 Protocol #{proto}" unless proto=~/^s3/
  object_name=File.join rest

  size=Integer( s3.buckets[bucket_name].objects[object_name].content_length )
  warn "Reading from #{bucket_name.inspect}, #{object_name.inspect}, size #{size}"
  ranges=(0..size).each_slice( CHUNKSIZE ).map {|ary| (ary.first..ary.last)}

  # Ruby GzipReader is unable to unzip these files, but unix gunzip can
  # Also means we don't need to eat much RAM, because everything is streaming.
  Open3.popen3( 'gunzip -c' ) {|sin,sout,serr,thr|

    # Create an ArcFile instance which will receive gunzip's stdout
    arcfile=ArcFileReader.new sout

    Thread.new do
      # Download chunks in the background and pipe them into gunzip
      # as we receive them
      ranges.each {|target_range|
        retry_count=5
        begin
          chunk=s3.buckets[bucket_name].objects[object_name].read( :range => target_range )
        rescue
          raise $! if (retry_count-=1)<0
          warn "Error (#{$!}) downloading #{target_range}, retrying."
          sleep 1 and retry
        end
        sin.write chunk
        Thread.pass
      }
      sin.close # which will send an EOF to the ArcFile
    end

    # Now we have a lazy ArcFile that we can treat as an Enumerable.
    arcfile.each {|url_record_headers, network_doc|
      process_document(url_record_headers, network_doc)
    }
  }
}
