describe Fluent::ElbAccessLogInput do
  let(:account_id) { '123456789012' }
  let(:s3_bucket) { 'my-bucket' }
  let(:s3_region) { 'us-west-1' }
  let(:driver) { create_driver(fluentd_conf) }
  let(:client){ Aws::S3::Client.new(stub_responses: true) }

  let(:fluentd_conf) do
    {
      account_id: account_id,
      s3_bucket: s3_bucket,
      s3_region: s3_region,
      start_datetime: (today - 1).to_s,
    }
  end

  let(:today) { Time.parse('2015/05/24 18:30 UTC') }
  let(:yesterday) { today - 86400 }
  let(:tomorrow) { today + 86400 }

  let(:today_prefix) { "AWSLogs/#{account_id}/elasticloadbalancing/#{s3_region}/#{today.strftime('%Y/%m/%d')}/" }
  let(:yesterday_prefix) { "AWSLogs/#{account_id}/elasticloadbalancing/#{s3_region}/#{yesterday.strftime('%Y/%m/%d')}/" }
  let(:tomorrow_prefix) { "AWSLogs/#{account_id}/elasticloadbalancing/#{s3_region}/#{tomorrow.strftime('%Y/%m/%d')}/" }

  let(:today_object_key) { "#{today_prefix}#{account_id}_elasticloadbalancing_ap-northeast-1_hoge_#{today.iso8601}_52.68.51.1_8hSqR3o4.log" }
  let(:yesterday_object_key) { "#{yesterday_prefix}#{account_id}_elasticloadbalancing_ap-northeast-1_hoge_#{yesterday.iso8601}_52.68.51.1_8hSqR3o4.log" }
  let(:tomorrow_object_key) { "#{tomorrow_prefix}#{account_id}_elasticloadbalancing_ap-northeast-1_hoge_#{tomorrow.iso8601}_52.68.51.1_8hSqR3o4.log" }

  before do
    Timecop.freeze(today)
    allow_any_instance_of(Fluent::ElbAccessLogInput).to receive(:client) { client }
    allow(FileUtils).to receive(:touch)
  end

  after do
    Timecop.return
  end

  subject { x = driver.emits; x }

  context 'when access log does not exist' do
    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }
      expect(driver.instance).to_not receive(:save_timestamp).with(today)

      driver.run
    end

    it { is_expected.to be_empty }
  end

  context 'when access log exists' do
    let(:today_access_log) do
      <<-EOS
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" - -
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" - -
      EOS
    end

    let(:tomorrow_access_log) do
      <<-EOS
2015-05-25T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" - -
2015-05-25T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" - -
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
        [double('today_s3_object', body: StringIO.new(today_access_log))]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: tomorrow_object_key) do
        [double('tomorrow_s3_object', body: StringIO.new(tomorrow_access_log))]
      end

      expect(driver.instance).to receive(:save_timestamp).with(tomorrow)

      driver.run
    end

    let(:expected_emits) do
      [["elb.access_log",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"timestamp"=>"2015-05-24T19:55:36.000000Z",
         "elb"=>"hoge",
         "client_port"=>"14.14.124.20:57673",
         "backend_port"=>"10.0.199.184:80",
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request.http_version"=>"HTTP/1.1",
         "request.uri.scheme"=>"http",
         "request.uri.userinfo"=>nil,
         "request.uri.host"=>"hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
         "request.uri.port"=>80,
         "request.uri.registry"=>nil,
         "request.uri.path"=>"/",
         "request.uri.opaque"=>nil,
         "request.uri.query"=>nil,
         "request.uri.fragment"=>nil}],
       ["elb.access_log",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"timestamp"=>"2015-05-24T19:55:36.000000Z",
         "elb"=>"hoge",
         "client_port"=>"14.14.124.20:57673",
         "backend_port"=>"10.0.199.184:80",
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request.http_version"=>"HTTP/1.1",
         "request.uri.scheme"=>"http",
         "request.uri.userinfo"=>nil,
         "request.uri.host"=>"hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
         "request.uri.port"=>80,
         "request.uri.registry"=>nil,
         "request.uri.path"=>"/",
         "request.uri.opaque"=>nil,
         "request.uri.query"=>nil,
         "request.uri.fragment"=>nil}],
       ["elb.access_log",
        Time.parse('2015-05-25 19:55:36 UTC').to_i,
        {"timestamp"=>"2015-05-25T19:55:36.000000Z",
         "elb"=>"hoge",
         "client_port"=>"14.14.124.20:57673",
         "backend_port"=>"10.0.199.184:80",
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request.http_version"=>"HTTP/1.1",
         "request.uri.scheme"=>"http",
         "request.uri.userinfo"=>nil,
         "request.uri.host"=>"hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
         "request.uri.port"=>80,
         "request.uri.registry"=>nil,
         "request.uri.path"=>"/",
         "request.uri.opaque"=>nil,
         "request.uri.query"=>nil,
         "request.uri.fragment"=>nil}],
       ["elb.access_log",
        Time.parse('2015-05-25 19:55:36 UTC').to_i,
        {"timestamp"=>"2015-05-25T19:55:36.000000Z",
         "elb"=>"hoge",
         "client_port"=>"14.14.124.20:57673",
         "backend_port"=>"10.0.199.184:80",
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request.http_version"=>"HTTP/1.1",
         "request.uri.scheme"=>"http",
         "request.uri.userinfo"=>nil,
         "request.uri.host"=>"hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
         "request.uri.port"=>80,
         "request.uri.registry"=>nil,
         "request.uri.path"=>"/",
         "request.uri.opaque"=>nil,
         "request.uri.query"=>nil,
         "request.uri.fragment"=>nil}]]
    end

    it { is_expected.to eq expected_emits }
  end

  context 'when access log exists (with tag option)' do
    let(:today_access_log) do
      <<-EOS
2015-05-24T19:55:36.000000Z hoge 14.14.124.20:57673 10.0.199.184:80 0.000053 0.000913 0.000036 200 200 0 3 "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1" "curl/7.30.0" - -
      EOS
    end

  let(:fluentd_conf) do
    {
      account_id: account_id,
      s3_bucket: s3_bucket,
      s3_region: s3_region,
      start_datetime: (today - 1).to_s,
      tag: 'any.tag'
    }
  end

    before do
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }

      expect(client).to receive(:list_objects).with(bucket: s3_bucket, prefix: today_prefix) do
        [double('today_objects', contents: [double('today_object', key: today_object_key)])]
      end

      expect(client).to receive(:get_object).with(bucket: s3_bucket, key: today_object_key) do
        [double('today_s3_object', body: StringIO.new(today_access_log))]
      end

      expect(driver.instance).to receive(:save_timestamp).with(today)

      driver.run
    end

    let(:expected_emits) do
      [["any.tag",
        Time.parse('2015-05-24 19:55:36 UTC').to_i,
        {"timestamp"=>"2015-05-24T19:55:36.000000Z",
         "elb"=>"hoge",
         "client_port"=>"14.14.124.20:57673",
         "backend_port"=>"10.0.199.184:80",
         "request_processing_time"=>5.3e-05,
         "backend_processing_time"=>0.000913,
         "response_processing_time"=>3.6e-05,
         "elb_status_code"=>200,
         "backend_status_code"=>200,
         "received_bytes"=>0,
         "sent_bytes"=>3,
         "request"=>
          "GET http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/ HTTP/1.1",
         "request.method"=>"GET",
         "request.uri"=>
          "http://hoge-1876938939.ap-northeast-1.elb.amazonaws.com:80/",
         "request.http_version"=>"HTTP/1.1",
         "request.uri.scheme"=>"http",
         "request.uri.userinfo"=>nil,
         "request.uri.host"=>"hoge-1876938939.ap-northeast-1.elb.amazonaws.com",
         "request.uri.port"=>80,
         "request.uri.registry"=>nil,
         "request.uri.path"=>"/",
         "request.uri.opaque"=>nil,
         "request.uri.query"=>nil,
         "request.uri.fragment"=>nil}]]
    end

    it { is_expected.to eq expected_emits }
  end
end
