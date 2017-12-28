describe 'Fluent::Plugin::ElbAccessLogInput#configure' do
  let(:account_id) { '123456789012' }
  let(:s3_bucket) { 'my-bucket' }
  let(:region) { 'us-west-1' }
  let(:driver) { create_driver(fluentd_conf) }
  let(:today) { Time.parse('2015/05/24 18:30 UTC') }

  subject { create_driver(fluentd_conf).instance }

  before do
    Timecop.freeze(today)
    allow(FileUtils).to receive(:touch)
    allow(File).to receive(:read) { nil }
    allow_any_instance_of(Fluent::Plugin::ElbAccessLogInput).to receive(:load_history) { [] }
    allow_any_instance_of(Fluent::Plugin::ElbAccessLogInput).to receive(:parse_tsfile) { nil }
  end

  context 'when default' do
    let(:fluentd_conf) do
      {
        account_id: account_id,
        s3_bucket: s3_bucket,
        region: region,
        interval: nil,
      }
    end

    it do
      expect(driver.instance.aws_key_id).to be_nil
      expect(driver.instance.aws_sec_key).to be_nil
      expect(driver.instance.profile).to be_nil
      expect(driver.instance.credentials_path).to be_nil
      expect(driver.instance.http_proxy).to be_nil
      expect(driver.instance.s3_bucket).to eq s3_bucket
      expect(driver.instance.region).to eq region
      expect(driver.instance.s3_prefix).to be_nil
      expect(driver.instance.tag).to eq 'elb.access_log'
      expect(driver.instance.tsfile_path).to eq '/var/tmp/fluent-plugin-elb-access-log.ts'
      expect(driver.instance.histfile_path).to eq '/var/tmp/fluent-plugin-elb-access-log.history'
      expect(driver.instance.interval).to eq 300
      expect(driver.instance.start_datetime).to eq today
      expect(driver.instance.buffer_sec).to eq 600
      expect(driver.instance.history_length).to eq 100
      expect(driver.instance.sampling_interval).to eq 1
      expect(driver.instance.debug).to be_falsey
      expect(driver.instance.elb_type).to eq 'clb'
    end
  end

  context 'when pass params' do
    let(:aws_key_id) { 'YOUR_ACCESS_KEY_ID' }
    let(:aws_sec_key) { 'YOUR_SECRET_ACCESS_KEY' }
    let(:profile) { 'PROFILE_NAME' }
    let(:credentials_path) { 'path/to/credentials_file' }
    let(:http_proxy) { 'http://example.net' }
    let(:s3_prefix) { 's3-prefix' }
    let(:tag) { 'any.tag' }
    let(:tsfile_path) { '/tmp/foo' }
    let(:histfile_path) { '/tmp/bar' }
    let(:interval) { 500 }
    let(:start_datetime) { today - 3600 }
    let(:buffer_sec) { 1200 }
    let(:history_length) { 200 }
    let(:sampling_interval) { 100 }
    let(:elb_type) { 'alb' }

    let(:fluentd_conf) do
      {
        aws_key_id: aws_key_id,
        aws_sec_key: aws_sec_key,
        profile: profile,
        credentials_path: credentials_path,
        http_proxy: http_proxy,
        account_id: account_id,
        s3_bucket: s3_bucket,
        region: region,
        s3_prefix: s3_prefix,
        tag: tag,
        tsfile_path: tsfile_path,
        histfile_path: histfile_path,
        interval: interval,
        start_datetime: start_datetime,
        buffer_sec: buffer_sec,
        history_length: history_length,
        sampling_interval: sampling_interval,
        debug: true,
        elb_type: elb_type,
      }
    end

    it do
      expect(driver.instance.aws_key_id).to eq aws_key_id
      expect(driver.instance.aws_sec_key).to eq aws_sec_key
      expect(driver.instance.profile).to eq profile
      expect(driver.instance.credentials_path).to eq credentials_path
      expect(driver.instance.http_proxy).to eq http_proxy
      expect(driver.instance.s3_bucket).to eq s3_bucket
      expect(driver.instance.region).to eq region
      expect(driver.instance.s3_prefix).to eq s3_prefix
      expect(driver.instance.tag).to eq tag
      expect(driver.instance.tsfile_path).to eq tsfile_path
      expect(driver.instance.histfile_path).to eq histfile_path
      expect(driver.instance.interval).to eq interval
      expect(driver.instance.start_datetime).to eq start_datetime
      expect(driver.instance.buffer_sec).to eq buffer_sec
      expect(driver.instance.history_length).to eq history_length
      expect(driver.instance.sampling_interval).to eq sampling_interval
      expect(driver.instance.debug).to be_truthy
      expect(driver.instance.elb_type).to eq elb_type
    end
  end

  context 'when start_datetime is set' do
    let(:start_datetime) { '2015-01-01 01:02:03 UTC' }

    let(:fluentd_conf) do
      {
        account_id: account_id,
        s3_bucket: s3_bucket,
        region: region,
        start_datetime: start_datetime
      }
    end

    it do
      expect(driver.instance.start_datetime).to eq Time.parse(start_datetime)
    end
  end

  context 'when tsfile datetime is set' do
    let(:tsfile_datetime) { '2015-02-01 01:02:03 UTC' }

    let(:fluentd_conf) do
      {
        account_id: account_id,
        s3_bucket: s3_bucket,
        region: region,
        start_datetime: tsfile_datetime
      }
    end

    before do
      allow_any_instance_of(Fluent::Plugin::ElbAccessLogInput).to receive(:parse_tsfile) { Time.parse(tsfile_datetime) }
    end

    it do
      expect(driver.instance.start_datetime).to eq Time.parse(tsfile_datetime)
    end
  end

  context 'when start_datetime and tsfile datetime are set' do
    let(:start_datetime) { '2015-01-01 01:02:03 UTC' }
    let(:tsfile_datetime) { '2015-02-01 01:02:03 UTC' }

    let(:fluentd_conf) do
      {
        account_id: account_id,
        s3_bucket: s3_bucket,
        region: region,
        start_datetime: start_datetime
      }
    end

    before do
      allow_any_instance_of(Fluent::Plugin::ElbAccessLogInput).to receive(:parse_tsfile) { Time.parse(tsfile_datetime) }
      allow_any_instance_of(Fluent::Test::TestLogger).to receive(:warn).with("start_datetime(#{start_datetime}) is set. but tsfile datetime(#{tsfile_datetime}) is used")
    end

    it do
      expect(driver.instance.start_datetime).to eq Time.parse(tsfile_datetime)
    end
  end

  context 'when an invalid ELB type' do
    let(:start_datetime) { '2015-01-01 01:02:03 UTC' }

    let(:fluentd_conf) do
      {
        account_id: account_id,
        s3_bucket: s3_bucket,
        region: region,
        start_datetime: start_datetime,
        elb_type: 'invalid',
      }
    end

    it do
      expect {
        subject
      }.to raise_error 'Invalid ELB type: invalid'
    end
  end
end
