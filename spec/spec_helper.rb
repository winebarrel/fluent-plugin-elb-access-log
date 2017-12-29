require 'coveralls'
Coveralls.wear!

require 'fluent/test'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_elb_access_log'

require 'aws-sdk-s3'
require 'time'
require 'timecop'
require 'rspec/match_table'

# Disable Test::Unit
module Test::Unit::RunCount; def run(*); end; end

RSpec.configure do |config|
  config.before(:all) do
    Fluent::Test.setup
  end
end

def create_driver(options = {})
  options = {
    interval: 0,
  }.merge(options)

  account_id = options.fetch(:account_id) || '123456789012'
  s3_bucket = options.fetch(:s3_bucket) || 'my-bucket'
  region = options.fetch(:region) || 'us-west-1'

  additional_options = options.select {|k, v| v }.map {|key, value|
    "#{key} #{value}"
  }.join("\n")

  fluentd_conf = <<-EOS
type elb_access_log
account_id #{account_id}
s3_bucket #{s3_bucket}
region #{region}
#{additional_options}
  EOS

  Fluent::Test::Driver::Input.new(Fluent::Plugin::ElbAccessLogInput).configure(fluentd_conf)
end

def driver_run(driver)
  driver.run do
    coolio_loop = driver.instance.instance_variable_get(:@loop)
    sleep 0.1 until coolio_loop.instance_variable_get(:@running)
  end
end

def gzip(str)
  io = StringIO.new

  Zlib::GzipWriter.wrap(io) do |gz|
    gz << str
  end

  io.string
end

# prevent Test::Unit's AutoRunner from executing during RSpec's rake task
# ref: https://github.com/rspec/rspec-rails/issues/1171
Test::Unit.run = true if defined?(Test::Unit) && Test::Unit.respond_to?(:run=)
