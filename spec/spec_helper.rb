require 'fluent/test'
require 'fluent/plugin/in_elb_access_log'

require 'aws-sdk-s3'
require 'time'
require 'timecop'
require 'rspec/match_table'

if ENV['TRAVIS']
  require 'simplecov'
  require 'coveralls'

  SimpleCov.formatter = Coveralls::SimpleCov::Formatter
  SimpleCov.start do
    add_filter "spec/"
  end
end

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

  tag = options[:tag] || 'test.default'
  Fluent::Test::OutputTestDriver.new(Fluent::ElbAccessLogInput, tag).configure(fluentd_conf)
end

# prevent Test::Unit's AutoRunner from executing during RSpec's rake task
# ref: https://github.com/rspec/rspec-rails/issues/1171
Test::Unit.run = true if defined?(Test::Unit) && Test::Unit.respond_to?(:run=)
