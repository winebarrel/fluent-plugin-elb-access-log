# fluent-plugin-elb-access-log

Fluentd input plugin for [AWS ELB Access Logs](http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html).

[![Gem Version](https://badge.fury.io/rb/fluent-plugin-elb-access-log.svg)](http://badge.fury.io/rb/fluent-plugin-elb-access-log)
[![Build Status](https://travis-ci.org/winebarrel/fluent-plugin-elb-access-log.svg?branch=master)](https://travis-ci.org/winebarrel/fluent-plugin-elb-access-log)

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
  type elb_access_log
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
</source>
```

```javascript
// elb.access_log:
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

# Difference with [fluent-plugin-elb-log](https://github.com/shinsaka/fluent-plugin-elb-log)

* Use AWS SDK for Ruby V2.
* It is possible to change the record tag.
* List objects with prefix.
* Perse request line URI.
