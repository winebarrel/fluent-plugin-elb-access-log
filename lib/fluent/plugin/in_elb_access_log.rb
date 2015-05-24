require 'fluent_plugin_elb_access_log/version'

class Fluent::ElbAccessLogInput < Fluent::Input
  Fluent::Plugin.register_input('elb_access_log', self)

  USER_AGENT_SUFFIX = "fluent-plugin-elb-access-log/#{FluentPluginElbAccessLog::VERSION}"

  # http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format
  ACCESS_LOG_FIELDS = {
    'timestamp'                => nil,
    'elb'                      => nil,
    'client_port'              => nil,
    'backend_port'             => nil,
    'request_processing_time'  => :to_f,
    'backend_processing_time'  => :to_f,
    'response_processing_time' => :to_f,
    'elb_status_code'          => :to_i,
    'backend_status_code'      => :to_i,
    'received_bytes'           => :to_i,
    'sent_bytes'               => :to_i,
    'request'                  => nil,
  }

  unless method_defined?(:log)
    define_method('log') { $log }
  end

  unless method_defined?(:router)
    define_method('router') { Fluent::Engine }
  end


  config_param :aws_key_id,       :string,  :default => nil
  config_param :aws_sec_key,      :string,  :default => nil
  config_param :profile,          :string,  :default => nil
  config_param :credentials_path, :string,  :default => nil
  config_param :account_id,       :string
  config_param :s3_bucket,        :string
  config_param :s3_region,        :string
  config_param :s3_prefix,        :string,  :default => nil
  config_param :tag,              :string,  :default => 'elb.access_log'
  config_param :tsfile_path,      :string,  :default => '/var/tmp/fluent-plugin-elb-access-log.ts'
  config_param :interval,         :time,    :default => 300
  config_param :start_datetime,   :string,  :default => nil
  config_param :debug,            :bool,    :default => false

  def initialize
    super
    require 'csv'
    require 'fileutils'
    require 'logger'
    require 'time'
    require 'aws-sdk'
  end

  def configure(conf)
    super

    FileUtils.touch(@tsfile_path)

    if @start_datetime
      @start_datetime = Time.parse(@start_datetime).utc
    else
      @start_datetime = Time.parse(File.read(@tsfile_path)).utc rescue Time.now.utc
    end
  end

  def start
    super

    # Load client
    client

    @loop = Coolio::Loop.new
    timestamp = @start_datetime

    timer = TimerWatcher.new(@interval, true, log) do
      new_timestamp = fetch(timestamp)

      if timestamp != new_timestamp
        save_timestamp(new_timestamp)
        timestamp = new_timestamp
      end
    end

    @loop.attach(timer)
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @loop.stop
    @thread.join
  end

  private

  def run
    @loop.run
  rescue => e
    log.error(e.message)
    log.error_backtrace(e.backtrace)
  end

  def fetch(timestamp)
    last_timestamp = timestamp

    prefixes(timestamp).each do |prefix|
      client.list_objects(:bucket => @s3_bucket, :prefix => prefix).each do |page|
        page.contents.each do |obj|
          account_id, logfile_const, region, elb_name, logfile_datetime, ip, logfile_suffix = obj.key.split('_', 7)
          logfile_datetime = Time.parse(logfile_datetime)

          if logfile_suffix !~ /\.log\z/ or logfile_datetime <= timestamp
            next
          end

          access_log = client.get_object(bucket: @s3_bucket, key: obj.key).first.body.string
          emit_access_log(access_log)
          last_timestamp = logfile_datetime
        end
      end
    end

    last_timestamp
  end

  def prefixes(timestamp)
    base_prefix = "AWSLogs/#{@account_id}/elasticloadbalancing/#{@s3_region}/"
    base_prefix = "#{@s3_prefix}/#{base_prefix}" if @s3_prefix

    [timestamp, timestamp + 86400].map do |date|
      base_prefix + date.strftime('%Y/%m/%d/')
    end
  end

  def emit_access_log(access_log)
    access_log = CSV.parse(access_log, :col_sep => ' ')

    access_log.each do |row|
      record = Hash[ACCESS_LOG_FIELDS.keys.zip(row)]

      ACCESS_LOG_FIELDS.each do |name, conv|
        record[name] = record[name].send(conv) if conv
      end

      time = Time.parse(record['timestamp'])
      router.emit(@tag, time, record)
    end
  end

  def save_timestamp(timestamp)
    open(@tsfile_path, 'w') do |tsfile|
      tsfile << timestamp.to_s
    end
  end

  def client
    return @client if @client

    options = {:user_agent_suffix => USER_AGENT_SUFFIX}
    options[:region] = @region if @region

    if @aws_key_id and @aws_sec_key
      options[:access_key_id] = @aws_key_id
      options[:secret_access_key] = @aws_sec_key
    elsif @profile
      credentials_opts = {:profile_name => @profile}
      credentials_opts[:path] = @credentials_path if @credentials_path
      credentials = Aws::SharedCredentials.new(credentials_opts)
      options[:credentials] = credentials
    end

    if @debug
      options[:logger] = Logger.new(log.out)
      options[:log_level] = :debug
      #options[:http_wire_trace] = true
    end

    @client = Aws::S3::Client.new(options)
  end

  class TimerWatcher < Coolio::TimerWatcher
    def initialize(interval, repeat, log, &callback)
      @callback = callback
      @log = log
      super(interval, repeat)
    end

    def on_timer
      @callback.call
    rescue => e
      @log.error(e.message)
      @log.error_backtrace(e.backtrace)
    end
  end # TimerWatcher
end # Fluent::ElbAccessLogInput
