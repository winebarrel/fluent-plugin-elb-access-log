# fluent-plugin-elb-access-log

Fluentd input plugin for AWS ELB Access Logs.

[![Gem Version](https://badge.fury.io/rb/fluent-plugin-elb-access-log.svg)](http://badge.fury.io/rb/fluent-plugin-elb-access-log)
[![Build Status](https://travis-ci.org/winebarrel/fluent-plugin-elb-access-log.svg?branch=master)](https://travis-ci.org/winebarrel/fluent-plugin-elb-access-log)
[![Coverage Status](https://coveralls.io/repos/github/winebarrel/fluent-plugin-elb-access-log/badge.svg?branch=master)](https://coveralls.io/github/winebarrel/fluent-plugin-elb-access-log?branch=master)

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'fluent-plugin-elb-access-log'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install fluent-plugin-elb-access-log

## Configuration

```apache
<source>
  @type elb_access_log
  #aws_key_id YOUR_ACCESS_KEY_ID
  #aws_sec_key YOUR_SECRET_ACCESS_KEY
  #profile PROFILE_NAME
  #credentials_path path/to/credentials_file
  #http_proxy http://...

  account_id 123456789012 # required
  region us-west-1 # required
  s3_bucket BUCKET_NAME # required
  #s3_prefix PREFIX

  #tag elb.access_log
  #tsfile_path /var/tmp/fluent-plugin-elb-access-log.ts
  #histfile_path /var/tmp/fluent-plugin-elb-access-log.history
  #interval 300
  #start_datetime 2015/05/24 17:00
  #buffer_sec 600
  #history_length 100
  #sampling_interval 1
  #debug false
  #elb_type clb # or alb
  #filter elb_status_code:^2,timestamp:^2018
  #filter_operator and # or "or"
  #type_cast true
  #parse_request true
  #split_addr_port true
  #file_filter REGEXP
  #request_separator .
</source>
```

## Outout

### CLB

see http://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html

```json
{
  "timestamp":"2015-05-24T08:25:36.229576Z",
  "elb":"hoge",
  "client":"14.14.124.20",
  "client_port":52232,
  "backend":"10.0.199.184",
  "backend_port":80,
  "request_processing_time":5.5e-05,
  "backend_processing_time":0.000893,
  "response_processing_time":5.7e-05,
  "elb_status_code":200,
  "backend_status_code":200,
  "received_bytes":0,
  "sent_bytes":3,
  "request":"GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
  "user_agent":"curl/7.30.0",
  "ssl_cipher":"-",
  "ssl_protocol":"-",
  "request.method":"GET",
  "request.uri":"http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
  "request.http_version":"HTTP/1.1",
  "request.uri.scheme":"http",
  "request.uri.user":null,
  "request.uri.host":"hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
  "request.uri.port":80,
  "request.uri.path":"/",
  "request.uri.query":null,
  "request.uri.fragment":null
}
```

### ALB

see http://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html

```json
{
  "type": "https",
  "timestamp": "2015-05-24T19:55:36.000000Z",
  "elb": "hoge",
  "client_port": 57673,
  "target_port": 80,
  "request_processing_time": 5.3e-05,
  "target_processing_time": 0.000913,
  "response_processing_time": 3.6e-05,
  "elb_status_code": 200,
  "target_status_code": 200,
  "received_bytes": 0,
  "sent_bytes": 3,
  "request": "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
  "user_agent": "curl/7.30.0",
  "ssl_cipher": "ssl_cipher",
  "ssl_protocol": "ssl_protocol",
  "target_group_arn": "arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx",
  "trace_id": "Root=xxx",
  "domain_name": "-",
  "chosen_cert_arn": "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx",
  "client": "14.14.124.20",
  "target": "10.0.199.184",
  "request.method": "GET",
  "request.uri": "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
  "request.http_version": "HTTP/1.1",
  "request.uri.scheme": "http",
  "request.uri.user": null,
  "request.uri.host": "hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
  "request.uri.port": 80,
  "request.uri.path": "/",
  "request.uri.query": null,
  "request.uri.fragment": null
}
```
