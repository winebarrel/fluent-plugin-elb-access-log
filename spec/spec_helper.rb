require 'fluent/test'
require 'fluent/plugin/in_elb_access_log'

require 'aws-sdk'
require 'time'
require 'timecop'

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
  s3_region = options.fetch(:s3_region) || 'us-west-1'

  additional_options = options.map {|key, value|
    "#{key} #{value}"
  }.join("\n")

  fluentd_conf = <<-EOS
type elb_access_log
account_id #{account_id}
s3_bucket #{s3_bucket}
s3_region #{s3_region}
#{additional_options}
  EOS

  tag = options[:tag] || 'test.default'
  Fluent::Test::OutputTestDriver.new(Fluent::ElbAccessLogInput, tag).configure(fluentd_conf)
end
