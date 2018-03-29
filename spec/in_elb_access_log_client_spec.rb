describe 'FluentPluginElbAccessLogInput#client' do
  let(:account_id) { '123456789012' }
  let(:s3_bucket) { 'my-bucket' }
  let(:region) { 'us-west-1' }
  let(:driver) { create_driver(fluentd_conf) }
  let!(:client){ Aws::S3::Client.new(stub_responses: true) }

  let(:fluentd_conf) do
    {
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
    allow_any_instance_of(FluentPluginElbAccessLogInput).to receive(:load_history) { [] }
    allow_any_instance_of(FluentPluginElbAccessLogInput).to receive(:parse_tsfile) { nil }
    expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: yesterday_prefix) { [] }
    expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: today_prefix) { [] }
    expect(client).to receive(:list_objects_v2).with(bucket: s3_bucket, prefix: tomorrow_prefix) { [] }
    allow(FileUtils).to receive(:touch)
    expect(driver.instance).to_not receive(:save_timestamp).with(today)
    expect(driver.instance).to receive(:save_history)
    expect(driver.instance.log).to_not receive(:error)
    expect(driver.instance.log).to_not receive(:warn)
  end

  after do
    Timecop.return
  end

  context 'when create client without credentials' do
    specify do
      expect(Aws::S3::Client).to receive(:new).with(
        region: region,
        user_agent_suffix: FluentPluginElbAccessLogInput::USER_AGENT_SUFFIX,
      ).and_return(client)

      driver_run(driver)
    end
  end

  context 'when create client with aws_key_id/aws_sec_key' do
    let(:aws_key_id) { 'akid' }
    let(:aws_sec_key) { 'secret' }

    let(:fluentd_conf) do
      {
        account_id: account_id,
        s3_bucket: s3_bucket,
        region: region,
        start_datetime: (today - 1).to_s,
        aws_key_id: aws_key_id,
        aws_sec_key: aws_sec_key,
      }
    end

    specify do
      expect(Aws::S3::Client).to receive(:new).with(
        region: region,
        user_agent_suffix: FluentPluginElbAccessLogInput::USER_AGENT_SUFFIX,
        access_key_id: aws_key_id,
        secret_access_key: aws_sec_key,
      ).and_return(client)

      driver_run(driver)
    end
  end

  context 'when create client with profile/credentials_path' do
    let(:profile) { 'my-profile' }
    let(:credentials_path) { '/foo/bar/zoo' }

    let(:fluentd_conf) do
      {
        account_id: account_id,
        s3_bucket: s3_bucket,
        region: region,
        start_datetime: (today - 1).to_s,
        profile: profile,
        credentials_path: credentials_path,
      }
    end

    specify do
      expect(Aws::S3::Client).to receive(:new) do |options|
        credentials = options.fetch(:credentials)
        expect(credentials.profile_name).to eq profile
        expect(credentials.path).to eq credentials_path
        client
      end

      driver_run(driver)
    end
  end

  context 'when create client with debug' do
    let(:fluentd_conf) do
      {
        account_id: account_id,
        s3_bucket: s3_bucket,
        region: region,
        start_datetime: (today - 1).to_s,
        debug: true,
      }
    end

    specify do
      expect(Aws::S3::Client).to receive(:new) do |options|
        expect(options.fetch(:log_level)).to eq :debug
        expect(options.fetch(:logger)).to be_a(Logger)
        client
      end

      driver_run(driver)
    end
  end
end
