#!/usr/bin/env ruby
require 'aws-sdk'

client = Aws::S3::Client.new
object = client.get_object(bucket:"winebarrel.log",key:"AWSLogs/822997939312/elasticloadbalancing/ap-northeast-1/2015/06/27/822997939312_elasticloadbalancing_ap-northeast-1_elb-test_20150627T1210Z_54.249.107.201_28i6l2LG.log")
p object.first

