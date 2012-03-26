#! /usr/bin/env ruby

require 'rubygems'
require 'json'

# Inline this so we don't have to copy a file while bootstrapping
# See http://www.archive.org/web/researcher/ArcFileFormat.php
# for information about the ARC format
# This class writes out ARC format files, with the header line
# output on initialization, and then subsequent calls to .write_doc
# output chunks for each individual document
class ArcFileWriter
  # Pass in a file handle, and any options you want to customize
  def initialize( output_stream, input_options = {} )
    @handle = output_stream
    # These are the default options, you probably won't need to change them
    @options = {
      'filename' => 'filedesc://unknown.arc.gz',
      'ip_address' => '0.0.0.0',
      'filedate' => Time.now.utc.strftime("%Y%m%d%H%M%S"),
      'filetype' => 'text/plain',
      'version' => '1',
      'reserved' => '0',
      'origin_code' => 'CommonCrawl',
      'doc_header_columns' => [
        'URL',
        'IP-address',
        'Archive-date',
        'Content-type',
        'Archive-length',
      ],
    }
    # Override the defaults with any user-supplied values
    input_options.each do |name, value|
      @options[name] = value
    end
    # Pull the options into instance variables for convenience
    @filename = @options['filename']
    @ip_address = @options['ip_address']
    @filedate = @options['filedate']
    @filetype = @options['filetype']
    @version = @options['version']
    @reserved = @options['reserved']
    @origin_code = @options['origin_code']
    @doc_header_columns = @options['doc_header_columns']
    
    # The version block body should look like this:
    #1 0 CommonCrawl
    #URL IP-address Archive-date Content-type Archive-length
    version_block_string = [
      [@version, @reserved, @origin_code].join(' '),
      @doc_header_columns.join(' '),
      '',
    ].join("\n")+"\n"
    version_block_length = version_block_string.length
    # The version block header should look like this:
    #filedesc://1258794327404_0.arc.gz 0.0.0.0 20091121010527 text/plain 73
    version_block_header = [
      @filename, 
      @ip_address, 
      @filedate, 
      @filetype, 
      version_block_length,
    ].join(' ') + "\n"
    @handle.write(version_block_header)
    @handle.write(version_block_string)
  end

  # Call this when you've got a new document to append to the ARC file
  def write_doc(url_record_headers, network_doc)
    # Override any old stored length with the current one
    network_doc_length = network_doc.length
    url_record_headers['Archive-length'] = network_doc_length
    # Go through all the doc headers and either use empty strings or
    # the client-supplied values
    url_record_list = []
    @doc_header_columns.each do |name|
      if url_record_headers[name]
        value = url_record_headers[name]
      else
        value = ''
      end
      url_record_list << value
    end
    # The url record should look like this:
    #http://mendozaopina.blogspot.com/ 209.85.229.191 20091103192002 text/html 298216
    url_record_string = url_record_list.join(' ')+"\n"
    @handle.write(url_record_string)
    @handle.write(network_doc)
    @handle.write("\n")
  end

end

# A special value used to store the status line from a GET result
STATUS_HEADER = 'x_commoncrawl_Status'

# Takes the pieces of a network document, and stitches them back together into a single
# string that looks like the result of an HTTP GET
def components2doc(headers, content)
  headers_list = []
  # Add on the status line first, if there is one
  if headers[STATUS_HEADER]
    headers_list << headers[STATUS_HEADER]
  else
    headers_list << 'HTTP/1.1 200 OK'
  end
  # Assemble the headers after the status line
  headers.each do |name, value|
    # Skip our special status line header
    if name == STATUS_HEADER then next end
    headers_list << [name, value].join(':')
  end
  headers_string = headers_list.join("\n")
  # Output the headers, followed by an empty line, then the content
  headers_string + "\n" + content
end

# Write the output as a plain ARC file. You'll probably want to gzip it, but that
# can be handled by specifying these arguments when you create the job:
# -jobconf mapred.output.compress=true 
# -jobconf mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCode
arcfile = ArcFileWriter.new $stdout

# Each document is output on its own line, as a key, tab, then a JSON string
ARGF.each do |line|
  key, value_string = line.split("\t", 2)
  # Pull the document information from the JSON
  value = JSON.parse(value_string)
  url_record_headers = value['url_record_headers']
  network_doc_object = value['network_doc']
  headers = network_doc_object['headers']
  content = network_doc_object['content']
  # Convert the document to a plain string
  network_doc_string = components2doc(headers, content)
  # Append the document string to the ARC file
  arcfile.write_doc(url_record_headers, network_doc_string)
end
