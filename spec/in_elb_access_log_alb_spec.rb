describe Fluent::Plugin::ElbAccessLogInput do
  let(:account_id) { '123456789012' }
  let(:s3_bucket) { 'my-bucket' }
  let(:region) { 'us-west-1' }
  let(:driver) { create_driver(fluentd_conf) }
  let!(:client){ Aws::S3::Client.new(stub_responses: true) }

  let(:fluentd_conf) do
    {
      interval: 0,
      account_id: account_id,
      s3_bucket: s3_bucket,
      region: region,
      start_datetime: (today - 1).to_s,
      elb_type: 'alb',
    }
  end

  let(:today) { Time.parse('2015/05/24 18:30 UTC') }
  let(:yesterday) { today - 86400 }
  let(:tomorrow) { today + 86400 }

  let(:today_prefix) { "AWSLogs/#{account_id}/elasticloadbalancing/#{region}/#{today.strftime('%Y/%m/%d')}/" }
  let(:yesterday_prefix) { "AWSLogs/#{account_id}/elasticloadbalancing/#{region}/#{yesterday.strftime('%Y/%m/%d')}/" }
  let(:tomorrow_prefix) { "AWSLogs/#{account_id}/elasticloadbalancing/#{region}/#{tomorrow.strftime('%Y/%m/%d')}/" }

  let(:today_object_key) { "#{today_prefix}#{account_id}_elasticloadbalancing_ap-northeast-1_hoge_#{today.iso8601}_52.68.51.1_8hSqR3o4.log.gz" }
  let(:yesterday_object_key) { "#{yesterday_prefix}#{account_id}_elasticloadbalancing_ap-northeast-1_hoge_#{yesterday.iso8601}_52.68.51.1_8hSqR3o4.log.gz" }
  let(:tomorrow_object_key) { "#{tomorrow_prefix}#{account_id}_elasticloadbalancing_ap-northeast-1_hoge_#{tomorrow.iso8601}_52.68.51.1_8hSqR3o4.log.gz" }

  before do
    Timecop.freeze(today)
    allow(Aws::S3::Client).to receive(:new) { client }
    allow_any_instance_of(Fluent::Plugin::ElbAccessLogInput).to receive(:load_history) { [] }
    allow_any_instance_of(Fluent::Plugin::ElbAccessLogInput).to receive(:parse_tsfile) { nil }
    allow(FileUtils).to receive(:touch)
    expect(driver.instance.log).to_not receive(:error)
  end

  after do
    Timecop.return
  end

  subject { driver.events }

  context 'when access log does not exist' do
    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }
      expect(driver.instance).to_not receive(:save_timestamp).with(today)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    it { is_expected.to be_empty }
  end

  context 'when access log exists' do
    let(:today_access_log) do
      gzip(<<-EOS)
https 2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
https 2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
      EOS
    end

    let(:tomorrow_access_log) do
      gzip(<<-EOS)
https 2015-05-25T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
https 2015-05-25T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
      EOS
    end

    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) do
        [double('yesterday_objects', contents: [double('yesterday_object', key: yesterday_object_key)])]
      end

      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) do
        [double('tomorrow_objects', contents: [double('tomorrow_object', key: tomorrow_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: tomorrow_object_key) do
        double('tomorrow_s3_object', body: StringIO.new(tomorrow_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(tomorrow)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    let(:expected_emits) do
      [["elb.access_log",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"type"=>"https",
         "timestamp"=>"2015-05-24T19:55:36.000000Z",
         "elb"=>"hoge",
         "client_port"=>57673,
         "target_port"=>80,
         "request_processing_time"=>5.3e-05,
         "target_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "target_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>"curl/7.30.0",
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
         "target_group_arn"=>
          "arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx",
         "trace_id"=>"Root=xxx",
         "domain_name"=>"-",
         "chosen_cert_arn"=>
          "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx",
         "client"=>"14.14.124.20",
         "target"=>"10.0.199.184",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request.http_version"=>"HTTP/1.1",
         "request.uri.scheme"=>"http",
         "request.uri.user"=>nil,
         "request.uri.host"=>"hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
         "request.uri.port"=>80,
         "request.uri.path"=>"/",
         "request.uri.query"=>nil,
         "request.uri.fragment"=>nil}],
       ["elb.access_log",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"type"=>"https",
         "timestamp"=>"2015-05-24T19:55:36.000000Z",
         "elb"=>"hoge",
         "client_port"=>57673,
         "target_port"=>80,
         "request_processing_time"=>5.3e-05,
         "target_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "target_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>"curl/7.30.0",
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
         "target_group_arn"=>
          "arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx",
         "trace_id"=>"Root=xxx",
         "domain_name"=>"-",
         "chosen_cert_arn"=>
          "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx",
         "client"=>"14.14.124.20",
         "target"=>"10.0.199.184",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request.http_version"=>"HTTP/1.1",
         "request.uri.scheme"=>"http",
         "request.uri.user"=>nil,
         "request.uri.host"=>"hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
         "request.uri.port"=>80,
         "request.uri.path"=>"/",
         "request.uri.query"=>nil,
         "request.uri.fragment"=>nil}],
       ["elb.access_log",
        Time.parse('2015-05-25 19:55:36 UTC').to_i,
        {"type"=>"https",
         "timestamp"=>"2015-05-25T19:55:36.000000Z",
         "elb"=>"hoge",
         "client_port"=>57673,
         "target_port"=>80,
         "request_processing_time"=>5.3e-05,
         "target_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "target_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>"curl/7.30.0",
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
         "target_group_arn"=>
          "arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx",
         "trace_id"=>"Root=xxx",
         "domain_name"=>"-",
         "chosen_cert_arn"=>
          "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx",
         "client"=>"14.14.124.20",
         "target"=>"10.0.199.184",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request.http_version"=>"HTTP/1.1",
         "request.uri.scheme"=>"http",
         "request.uri.user"=>nil,
         "request.uri.host"=>"hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
         "request.uri.port"=>80,
         "request.uri.path"=>"/",
         "request.uri.query"=>nil,
         "request.uri.fragment"=>nil}],
       ["elb.access_log",
        Time.parse('2015-05-25 19:55:36 UTC').to_i,
        {"type"=>"https",
         "timestamp"=>"2015-05-25T19:55:36.000000Z",
         "elb"=>"hoge",
         "client_port"=>57673,
         "target_port"=>80,
         "request_processing_time"=>5.3e-05,
         "target_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "target_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>"curl/7.30.0",
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
         "target_group_arn"=>
          "arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx",
         "trace_id"=>"Root=xxx",
         "domain_name"=>"-",
         "chosen_cert_arn"=>
          "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx",
         "client"=>"14.14.124.20",
         "target"=>"10.0.199.184",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request.http_version"=>"HTTP/1.1",
         "request.uri.scheme"=>"http",
         "request.uri.user"=>nil,
         "request.uri.host"=>"hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
         "request.uri.port"=>80,
         "request.uri.path"=>"/",
         "request.uri.query"=>nil,
         "request.uri.fragment"=>nil}]]
    end

    it { is_expected.to match_table expected_emits }

    context 'when sampling' do
      let(:fluentd_conf) do
        {
          interval: 0,
          account_id: account_id,
          s3_bucket: s3_bucket,
          region: region,
          start_datetime: (today - 1).to_s,
          sampling_interval: 2,
          elb_type: 'alb',
        }
      end

      it do
        expected_emits.delete_at(3)
        expected_emits.delete_at(1)
        is_expected.to match_table expected_emits
      end
    end

    context 'with filter' do
      let(:fluentd_conf) do
        {
          interval: 0,
          account_id: account_id,
          s3_bucket: s3_bucket,
          region: region,
          start_datetime: (today - 1).to_s,
          elb_type: 'alb',
          filter: '{"timestamp": "2015-05-25"}',
        }
      end

      it do
        expected_emits.slice!(0, 2)
        is_expected.to match_table expected_emits
      end
    end

    context 'with filter (or)' do
      let(:fluentd_conf) do
        {
          interval: 0,
          account_id: account_id,
          s3_bucket: s3_bucket,
          region: region,
          start_datetime: (today - 1).to_s,
          elb_type: 'alb',
          filter: '{"timestamp": "2015-05-25"}',
          filter_operator: 'or'
        }
      end

      it do
        expected_emits.slice!(0, 2)
        is_expected.to match_table expected_emits
      end
    end

    context 'with filter (or/multi)' do
      let(:fluentd_conf) do
        {
          interval: 0,
          account_id: account_id,
          s3_bucket: s3_bucket,
          region: region,
          start_datetime: (today - 1).to_s,
          elb_type: 'alb',
          filter: '{"timestamp": "2015-05-25", "elb_status_code": "^2"}',
          filter_operator: 'or'
        }
      end

      it do
        is_expected.to match_table expected_emits
      end
    end
  end

  context 'when include bad URI' do
    let(:today_access_log) do
      gzip(<<-EOS)
https 2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
      EOS
    end

    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)

      allow(Addressable::URI).to receive(:parse).and_raise('parse error')
      expect(driver.instance.log).to receive(:warn).with('parse error: http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/')

      driver_run(driver)
    end

    let(:expected_emits) do
      [["elb.access_log",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"chosen_cert_arn"=>
          "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "domain_name"=>"-",
         "elb"=>"hoge",
         "elb_status_code"=>200,
         "received_bytes"=>0,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "request.http_version"=>"HTTP/1.1",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request_processing_time"=>5.3e-05,
         "response_processing_time"=>3.6e-05,
         "sent_bytes"=>3,
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
         "target"=>"10.0.199.184",
         "target_group_arn"=>
          "arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx",
         "target_port"=>80,
         "target_processing_time"=>0.000913,
         "target_status_code"=>200,
         "timestamp"=>"2015-05-24T19:55:36.000000Z",
         "trace_id"=>"Root=xxx",
         "type"=>"https",
         "user_agent"=>"curl/7.30.0"}]]
    end

    it { is_expected.to match_table expected_emits }
  end

  context 'when access log exists (with tag option)' do
    let(:today_access_log) do
      gzip(<<-EOS)
https 2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
      EOS
    end

    let(:fluentd_conf) do
      {
        interval: 0,
        account_id: account_id,
        s3_bucket: s3_bucket,
        region: region,
        start_datetime: (today - 1).to_s,
        tag: 'any.tag',
        elb_type: 'alb',
      }
    end

    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    let(:expected_emits) do
      [["any.tag",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"chosen_cert_arn"=>
          "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "domain_name"=>"-",
         "elb"=>"hoge",
         "elb_status_code"=>200,
         "received_bytes"=>0,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "request.http_version"=>"HTTP/1.1",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request.uri.fragment"=>nil,
         "request.uri.host"=>"hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
         "request.uri.path"=>"/",
         "request.uri.port"=>80,
         "request.uri.query"=>nil,
         "request.uri.scheme"=>"http",
         "request.uri.user"=>nil,
         "request_processing_time"=>5.3e-05,
         "response_processing_time"=>3.6e-05,
         "sent_bytes"=>3,
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
         "target"=>"10.0.199.184",
         "target_group_arn"=>
          "arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx",
         "target_port"=>80,
         "target_processing_time"=>0.000913,
         "target_status_code"=>200,
         "timestamp"=>"2015-05-24T19:55:36.000000Z",
         "trace_id"=>"Root=xxx",
         "type"=>"https",
         "user_agent"=>"curl/7.30.0"}]]
    end

    it { is_expected.to match_table expected_emits }
  end


  context 'when access old log exists' do
    let(:today_access_log) do
      gzip(<<-EOS)
https 2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
      EOS
    end

    let(:today_object_key) { "#{today_prefix}#{account_id}_elasticloadbalancing_ap-northeast-1_hoge_#{(today - 600).iso8601}_52.68.51.1_8hSqR3o4.log.gz" }

    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to_not receive(:save_timestamp)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    let(:expected_emits) do
      [["elb.access_log",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"chosen_cert_arn"=>
          "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "domain_name"=>"-",
         "elb"=>"hoge",
         "elb_status_code"=>200,
         "received_bytes"=>0,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "request.http_version"=>"HTTP/1.1",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request.uri.fragment"=>nil,
         "request.uri.host"=>"hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
         "request.uri.path"=>"/",
         "request.uri.port"=>80,
         "request.uri.query"=>nil,
         "request.uri.scheme"=>"http",
         "request.uri.user"=>nil,
         "request_processing_time"=>5.3e-05,
         "response_processing_time"=>3.6e-05,
         "sent_bytes"=>3,
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
         "target"=>"10.0.199.184",
         "target_group_arn"=>
          "arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx",
         "target_port"=>80,
         "target_processing_time"=>0.000913,
         "target_status_code"=>200,
         "timestamp"=>"2015-05-24T19:55:36.000000Z",
         "trace_id"=>"Root=xxx",
         "type"=>"https",
         "user_agent"=>"curl/7.30.0"}]]
    end

    it { is_expected.to match_table expected_emits }
  end

  context 'when parse error' do
    let(:today_access_log) do
      gzip(<<-EOS)
https 2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
      EOS
    end

    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)

      expect(CSV).to receive(:parse_line).and_raise('parse error')
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    let(:expected_emits) do
      [["elb.access_log",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"chosen_cert_arn"=>
          "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "domain_name"=>"-",
         "elb"=>"hoge",
         "elb_status_code"=>200,
         "received_bytes"=>0,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "request.http_version"=>"HTTP/1.1",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request.uri.fragment"=>nil,
         "request.uri.host"=>"hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
         "request.uri.path"=>"/",
         "request.uri.port"=>80,
         "request.uri.query"=>nil,
         "request.uri.scheme"=>"http",
         "request.uri.user"=>nil,
         "request_processing_time"=>5.3e-05,
         "response_processing_time"=>3.6e-05,
         "sent_bytes"=>3,
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
         "target"=>"10.0.199.184",
         "target_group_arn"=>
          "arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx",
         "target_port"=>80,
         "target_processing_time"=>0.000913,
         "target_status_code"=>200,
         "timestamp"=>"2015-05-24T19:55:36.000000Z",
         "trace_id"=>"Root=xxx",
         "type"=>"https",
         "user_agent"=>"curl/7.30.0"}]]
    end

    it { is_expected.to match_table expected_emits }

    context 'when no user_agent' do
      let(:today_access_log) do
        gzip(<<-EOS)
https 2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1"
        EOS
      end

      before do
        expected_emits[0][2]['user_agent'] = nil
        expected_emits[0][2]['ssl_cipher'] = nil
        expected_emits[0][2]['ssl_protocol'] = nil
        expected_emits[0][2]['target_group_arn'] = nil
        expected_emits[0][2]['trace_id'] = nil
        expected_emits[0][2]['domain_name'] = nil
        expected_emits[0][2]['chosen_cert_arn'] = nil
      end

      it { is_expected.to match_table expected_emits }
    end
  end

  context 'when access old log exists (timeout)' do
    let(:today_access_log) do
      gzip(<<-EOS)
https 2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
      EOS
    end

    let(:today_object_key) { "#{today_prefix}#{account_id}_elasticloadbalancing_ap-northeast-1_hoge_#{(today - 601).iso8601}_52.68.51.1_8hSqR3o4.log.gz" }

    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to_not receive(:get_object)
      expect(driver.instance).to_not receive(:save_timestamp)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    it { is_expected.to be_empty }
  end

  context 'when emitted log exists' do
    let(:today_access_log) do
      gzip(<<-EOS)
https 2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
      EOS
    end

    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to_not receive(:get_object)

      history = driver.instance.instance_variable_get(:@history)
      history << today_object_key
      expect(driver.instance).to_not receive(:save_timestamp)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    it { is_expected.to be_empty }
  end

  describe 'history#length' do
    let(:today_access_log) do
      gzip(<<-EOS)
https 2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
      EOS
    end

    let(:history) { driver.instance.instance_variable_get(:@history) }

    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)
      expect(driver.instance.log).to_not receive(:warn)
    end

    subject { history.length }

    context 'when history.length <= 100' do
      before do
        driver_run(driver)
      end

      it { is_expected.to eq 1 }
    end

    context 'when history.length > 100' do
      before do
        history.concat (1..100).map(&:to_s)
        driver_run(driver)
      end

      it { is_expected.to eq 100 }
    end
  end

  context 'when no user_agent' do
    let(:today_access_log) do
      gzip(<<-EOS)
https 2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
      EOS
    end

    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    let(:expected_emits) do
      [["elb.access_log",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"chosen_cert_arn"=>nil,
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "domain_name"=>nil,
         "elb"=>"hoge",
         "elb_status_code"=>200,
         "received_bytes"=>0,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "request.http_version"=>"HTTP/1.1",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request.uri.fragment"=>nil,
         "request.uri.host"=>"hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
         "request.uri.path"=>"/",
         "request.uri.port"=>80,
         "request.uri.query"=>nil,
         "request.uri.scheme"=>"http",
         "request.uri.user"=>nil,
         "request_processing_time"=>5.3e-05,
         "response_processing_time"=>3.6e-05,
         "sent_bytes"=>3,
         "ssl_cipher"=>"Root=xxx",
         "ssl_protocol"=>"-",
         "target"=>"10.0.199.184",
         "target_group_arn"=>
          "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx",
         "target_port"=>80,
         "target_processing_time"=>0.000913,
         "target_status_code"=>200,
         "timestamp"=>"2015-05-24T19:55:36.000000Z",
         "trace_id"=>nil,
         "type"=>"https",
         "user_agent"=>
          "arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx"}]]
    end

    it { is_expected.to match_table expected_emits }
  end

  context 'when inflate fails' do
    let(:today_access_log) do
      <<-EOS
https 2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
      EOS
    end

    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

    end

    specify do
      expect(driver.instance.log).to receive(:warn).with(/not in gzip format: /)
      driver_run(driver)
    end
  end

  context 'when bad timestamp' do
    let(:today_access_log) do
      gzip(<<-EOS)
https xxx hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
      EOS
    end

    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)
    end

    specify do
      expect(driver.instance.log).to receive(:warn).with(/no time information in "xxx":/)
      expect(driver.instance.log).to receive(:warn).with('A record that has bad timestamp is not emitted.')
      driver_run(driver)
    end
  end

  context 'when unquote fails' do
    let(:today_access_log) do
      gzip(<<-EOS)
https 2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol arn:aws:elasticloadbalancing:ap-northeast-1:123456789012:targetgroup/app/xxx "Root=xxx" "-" "arn:aws:acm:ap-northeast-1:123456789012:certificate/xxx"
      EOS
    end

    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)

      expect(CSV).to receive(:parse_line).and_raise('parse error')
      expect(driver.instance).to receive(:unquote).and_raise('unquote error')
    end

    specify do
      expect(driver.instance.log).to receive(:warn).with(/parse error: unquote error:/)
      driver_run(driver)
    end
  end
end
