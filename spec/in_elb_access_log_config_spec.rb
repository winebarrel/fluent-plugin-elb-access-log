describe 'Fluent::ElbAccessLogInput#configure' do
  let(:account_id) { '123456789012' }
  let(:s3_bucket) { 'my-bucket' }
  let(:s3_region) { 'us-west-1' }
  let(:driver) { create_driver(fluentd_conf) }
  let(:today) { Time.parse('2015/05/24 18:30 UTC') }

  subject { create_driver(fluentd_conf).instance }

  before do
    Timecop.freeze(today)
  end

  context 'when default' do
    let(:fluentd_conf) do
      {
        account_id: account_id,
        s3_bucket: s3_bucket,
        s3_region: s3_region,
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
      expect(driver.instance.s3_region).to eq s3_region
      expect(driver.instance.s3_prefix).to be_nil
      expect(driver.instance.tag).to eq 'elb.access_log'
      expect(driver.instance.tsfile_path).to eq '/var/tmp/fluent-plugin-elb-access-log.ts'
      expect(driver.instance.interval).to eq 300
      expect(driver.instance.start_datetime).to eq today
      expect(driver.instance.debug).to be_falsey
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
    let(:interval) { 500 }
    let(:start_datetime) { today - 3600 }

    let(:fluentd_conf) do
      {
        aws_key_id: aws_key_id,
        aws_sec_key: aws_sec_key,
        profile: profile,
        credentials_path: credentials_path,
        http_proxy: http_proxy,
        account_id: account_id,
        s3_bucket: s3_bucket,
        s3_region: s3_region,
        s3_prefix: s3_prefix,
        tag: tag,
        tsfile_path: tsfile_path,
        interval: interval,
        start_datetime: start_datetime,
        debug: true,
      }
    end

    it do
      expect(driver.instance.aws_key_id).to eq aws_key_id
      expect(driver.instance.aws_sec_key).to eq aws_sec_key
      expect(driver.instance.profile).to eq profile
      expect(driver.instance.credentials_path).to eq credentials_path
      expect(driver.instance.http_proxy).to eq http_proxy
      expect(driver.instance.s3_bucket).to eq s3_bucket
      expect(driver.instance.s3_region).to eq s3_region
      expect(driver.instance.s3_prefix).to eq s3_prefix
      expect(driver.instance.tag).to eq tag
      expect(driver.instance.tsfile_path).to eq tsfile_path
      expect(driver.instance.interval).to eq interval
      expect(driver.instance.start_datetime).to eq start_datetime
      expect(driver.instance.debug).to be_truthy
    end
  end
end
