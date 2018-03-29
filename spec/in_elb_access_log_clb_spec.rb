describe FluentPluginElbAccessLogInput do
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
    }
  end

  let(:today) { Time.parse('2015/05/24 18:30 UTC') }
  let(:yesterday) { today - 86400 }
  let(:tomorrow) { today + 86400 }

  let(:today_prefix) { "AWSLogs/#{account_id}/elasticloadbalancing/#{region}/#{today.strftime('%Y/%m/%d')}/" }
  let(:yesterday_prefix) { "AWSLogs/#{account_id}/elasticloadbalancing/#{region}/#{yesterday.strftime('%Y/%m/%d')}/" }
  let(:tomorrow_prefix) { "AWSLogs/#{account_id}/elasticloadbalancing/#{region}/#{tomorrow.strftime('%Y/%m/%d')}/" }

  let(:today_object_key) { "#{today_prefix}#{account_id}_elasticloadbalancing_ap-northeast-1_hoge_#{today.iso8601}_52.68.51.1_8hSqR3o4.log" }
  let(:yesterday_object_key) { "#{yesterday_prefix}#{account_id}_elasticloadbalancing_ap-northeast-1_hoge_#{yesterday.iso8601}_52.68.51.1_8hSqR3o4.log" }
  let(:tomorrow_object_key) { "#{tomorrow_prefix}#{account_id}_elasticloadbalancing_ap-northeast-1_hoge_#{tomorrow.iso8601}_52.68.51.1_8hSqR3o4.log" }

  before do
    Timecop.freeze(today)
    allow(Aws::S3::Client).to receive(:new) { client }
    allow_any_instance_of(FluentPluginElbAccessLogInput).to receive(:load_history) { [] }
    allow_any_instance_of(FluentPluginElbAccessLogInput).to receive(:parse_tsfile) { nil }
    allow(FileUtils).to receive(:touch)
    expect(driver.instance.log).to_not receive(:error)
  end

  after do
    Timecop.return
  end

  subject { driver_events }

  context 'when access log does not exist' do
    before do
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: today_prefix) { [] }
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }
      expect(driver.instance).to_not receive(:save_timestamp).with(today)
      expect(driver.instance).to receive(:save_history)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    it { is_expected.to be_empty }
  end

  context 'when access log exists' do
    let(:today_access_log) do
      <<-EOS
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
      EOS
    end

    let(:tomorrow_access_log) do
      <<-EOS
2015-05-25T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
2015-05-25T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
      EOS
    end

    before do
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: yesterday_prefix) do
        [double('yesterday_objects', contents: [double('yesterday_object', key: yesterday_object_key)])]
      end

      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: tomorrow_prefix) do
        [double('tomorrow_objects', contents: [double('tomorrow_object', key: tomorrow_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: tomorrow_object_key) do
        double('tomorrow_s3_object', body: StringIO.new(tomorrow_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(tomorrow)
      expect(driver.instance).to receive(:save_history)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    let(:expected_emits) do
      [["elb.access_log",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"timestamp"=>"2015-05-24T19:55:36.000000Z",
         "elb"=>"hoge",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "backend"=>"10.0.199.184",
         "backend_port"=>80,
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>"curl/7.30.0",
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
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
        {"timestamp"=>"2015-05-24T19:55:36.000000Z",
         "elb"=>"hoge",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "backend"=>"10.0.199.184",
         "backend_port"=>80,
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>"curl/7.30.0",
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
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
        {"timestamp"=>"2015-05-25T19:55:36.000000Z",
         "elb"=>"hoge",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "backend"=>"10.0.199.184",
         "backend_port"=>80,
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>"curl/7.30.0",
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
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
        {"timestamp"=>"2015-05-25T19:55:36.000000Z",
         "elb"=>"hoge",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "backend"=>"10.0.199.184",
         "backend_port"=>80,
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>"curl/7.30.0",
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
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
          filter: '{"timestamp": "2015-05-25"}',
          filter_operator: 'or',
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
          filter: '{"timestamp": "2015-05-25", "elb_status_code": "^2"}',
          filter_operator: 'or',
        }
      end

      it do
        is_expected.to match_table expected_emits
      end
    end

    context 'without type cast' do
      let(:fluentd_conf) do
        {
          interval: 0,
          account_id: account_id,
          s3_bucket: s3_bucket,
          region: region,
          start_datetime: (today - 1).to_s,
          type_cast: 'false',
        }
      end

      it do
        expected_emits_without_type_cast = expected_emits.map do |tag, ts, h|
          h = Hash[h.map {|k, v|
            v = case v
                when nil
                  v
                when Float
                  "%.6f" % v
                else
                  v.to_s
                end

            [k, v]
          }]

          [tag, ts, h]
        end

        is_expected.to match_table expected_emits_without_type_cast
      end
    end

    context 'without request parsing' do
      let(:fluentd_conf) do
        {
          interval: 0,
          account_id: account_id,
          s3_bucket: s3_bucket,
          region: region,
          start_datetime: (today - 1).to_s,
          parse_request: 'false',
        }
      end

      it do
        expected_emits_without_request_parsing = expected_emits.map do |tag, ts, h|
          h = Hash[h.select {|k, v| k !~ /\Arequest\./ }]
          [tag, ts, h]
        end

        is_expected.to match_table expected_emits_without_request_parsing
      end
    end

    context 'without addr/port splitting' do
      let(:fluentd_conf) do
        {
          interval: 0,
          account_id: account_id,
          s3_bucket: s3_bucket,
          region: region,
          start_datetime: (today - 1).to_s,
          split_addr_port: 'false',
        }
      end

      it do
        expected_emits_without_request_parsing = expected_emits.map do |tag, ts, h|
          h.keys.select {|k| k =~ /_port\z/ }.each do |prefix_port|
            prefix, _ = prefix_port.split('_', 2)
            h[prefix] = h[prefix] + ':' + h[prefix_port].to_s
            h.delete(prefix_port)
          end

          [tag, ts, h]
        end

        is_expected.to match_table expected_emits_without_request_parsing
      end
    end
  end

  context 'with file_filter' do
    let(:today_access_log) do
      <<-EOS
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
      EOS
    end

    let(:tomorrow_access_log) do
      <<-EOS
2015-05-25T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
2015-05-25T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
      EOS
    end

    before do
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: yesterday_prefix) do
        [double('yesterday_objects', contents: [double('yesterday_object', key: yesterday_object_key)])]
      end

      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: tomorrow_prefix) do
        [double('tomorrow_objects', contents: [double('tomorrow_object', key: tomorrow_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(client).to_not receive(:get_object).with(bucket: s3_bucket, key: tomorrow_object_key) do
        double('tomorrow_s3_object', body: StringIO.new(tomorrow_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)
      expect(driver.instance).to receive(:save_history)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    let(:expected_emits) do
      [["elb.access_log",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"timestamp"=>"2015-05-24T19:55:36.000000Z",
         "elb"=>"hoge",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "backend"=>"10.0.199.184",
         "backend_port"=>80,
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>"curl/7.30.0",
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
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
        {"timestamp"=>"2015-05-24T19:55:36.000000Z",
         "elb"=>"hoge",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "backend"=>"10.0.199.184",
         "backend_port"=>80,
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>"curl/7.30.0",
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
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

    let(:fluentd_conf) do
      {
        interval: 0,
        account_id: account_id,
        s3_bucket: s3_bucket,
        region: region,
        start_datetime: (today - 1).to_s,
        file_filter: today.iso8601,
      }
    end

    it { is_expected.to match_table expected_emits }
  end

  context 'when include bad URI' do
    let(:today_access_log) do
      <<-EOS
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
      EOS
    end

    before do
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)
      expect(driver.instance).to receive(:save_history)

      allow(Addressable::URI).to receive(:parse).and_raise('parse error')
      expect(driver.instance.log).to receive(:warn).with('parse error: http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/')

      driver_run(driver)
    end

    let(:expected_emits) do
      [["elb.access_log",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"timestamp"=>"2015-05-24T19:55:36.000000Z",
         "elb"=>"hoge",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "backend"=>"10.0.199.184",
         "backend_port"=>80,
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>"curl/7.30.0",
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request.http_version"=>"HTTP/1.1"}]]
    end

    it { is_expected.to match_table expected_emits }
  end

  context 'when access log exists (with tag option)' do
    let(:today_access_log) do
      <<-EOS
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
      EOS
    end

    let(:fluentd_conf) do
      {
        interval: 0,
        account_id: account_id,
        s3_bucket: s3_bucket,
        region: region,
        start_datetime: (today - 1).to_s,
        tag: 'any.tag'
      }
    end

    before do
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)
      expect(driver.instance).to receive(:save_history)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    let(:expected_emits) do
      [["any.tag",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"timestamp"=>"2015-05-24T19:55:36.000000Z",
         "elb"=>"hoge",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "backend"=>"10.0.199.184",
         "backend_port"=>80,
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>"curl/7.30.0",
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
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
  end

  context 'when access old log exists' do
    let(:today_access_log) do
      <<-EOS
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
      EOS
    end

    let(:today_object_key) { "#{today_prefix}#{account_id}_elasticloadbalancing_ap-northeast-1_hoge_#{(today - 600).iso8601}_52.68.51.1_8hSqR3o4.log" }

    before do
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to_not receive(:save_timestamp)
      expect(driver.instance).to receive(:save_history)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    let(:expected_emits) do
      [["elb.access_log",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"timestamp"=>"2015-05-24T19:55:36.000000Z",
         "elb"=>"hoge",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "backend"=>"10.0.199.184",
         "backend_port"=>80,
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>"curl/7.30.0",
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
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
  end

  context 'when parse error' do
    let(:today_access_log) do
      <<-EOS
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
      EOS
    end

    before do
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)
      expect(driver.instance).to receive(:save_history)

      expect(CSV).to receive(:parse_line).and_raise('parse error')
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    let(:expected_emits) do
      [["elb.access_log",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"timestamp"=>"2015-05-24T19:55:36.000000Z",
         "elb"=>"hoge",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "backend"=>"10.0.199.184",
         "backend_port"=>80,
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>"curl/7.30.0",
         "ssl_cipher"=>"ssl_cipher",
         "ssl_protocol"=>"ssl_protocol",
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

    context 'when no user_agent' do
      let(:today_access_log) do
        <<-EOS
  2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1"
        EOS
      end

      before do
        expected_emits[0][2]['user_agent'] = nil
        expected_emits[0][2]['ssl_cipher'] = nil
        expected_emits[0][2]['ssl_protocol'] = nil
      end

      it { is_expected.to match_table expected_emits }
    end
  end

  context 'when access old log exists (timeout)' do
    let(:today_access_log) do
      <<-EOS
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
      EOS
    end

    let(:today_object_key) { "#{today_prefix}#{account_id}_elasticloadbalancing_ap-northeast-1_hoge_#{(today - 601).iso8601}_52.68.51.1_8hSqR3o4.log" }

    before do
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to_not receive(:get_object)
      expect(driver.instance).to_not receive(:save_timestamp)
      expect(driver.instance).to receive(:save_history)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    it { is_expected.to be_empty }
  end

  context 'when emitted log exists' do
    let(:today_access_log) do
      <<-EOS
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
      EOS
    end

    before do
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to_not receive(:get_object)

      history = driver.instance.instance_variable_get(:@history)
      history << today_object_key
      expect(driver.instance).to_not receive(:save_timestamp)
      expect(driver.instance).to receive(:save_history)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    it { is_expected.to be_empty }
  end

  describe 'history#length' do
    let(:today_access_log) do
      <<-EOS
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
      EOS
    end

    let(:history) { driver.instance.instance_variable_get(:@history) }

    before do
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)
      expect(driver.instance).to receive(:save_history)
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
      <<-EOS
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1"
      EOS
    end

    before do
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)
      expect(driver.instance).to receive(:save_history)
      expect(driver.instance.log).to_not receive(:warn)

      driver_run(driver)
    end

    let(:expected_emits) do
      [["elb.access_log",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"timestamp"=>"2015-05-24T19:55:36.000000Z",
         "elb"=>"hoge",
         "client"=>"14.14.124.20",
         "client_port"=>57673,
         "backend"=>"10.0.199.184",
         "backend_port"=>80,
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "user_agent"=>nil,
         "ssl_cipher"=>nil,
         "ssl_protocol"=>nil,
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
  end

  context 'when no user_agent' do
    let(:today_access_log) do
      <<-EOS
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" ssl_cipher ssl_protocol
      EOS
    end

    before do
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        double('today_s3_object', body: StringIO.new(today_access_log))
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)
      expect(driver.instance).to receive(:save_history)

      expect(CSV).to receive(:parse_line).and_raise('parse error')
      expect(driver.instance).to receive(:unquote).and_raise('unquote error')
    end

    specify do
      expect(driver.instance.log).to receive(:warn).with(/parse error: unquote error:/)
      driver_run(driver)
    end
  end
end
