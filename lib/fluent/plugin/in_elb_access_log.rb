require 'fluent/input'
require 'fluent_plugin_elb_access_log/version'

class Fluent::Plugin::ElbAccessLogInput < Fluent::Plugin::Input
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
    'user_agent'               => nil,
    'ssl_cipher'               => nil,
    'ssl_protocol'             => nil,
  }

  unless method_defined?(:log)
    define_method('log') { $log }
  end

  unless method_defined?(:router)
    define_method('router') { Fluent::Engine }
  end

  config_param :aws_key_id,        :string,  :default => nil, :secret => true
  config_param :aws_sec_key,       :string,  :default => nil, :secret => true
  config_param :profile,           :string,  :default => nil
  config_param :credentials_path,  :string,  :default => nil
  config_param :http_proxy,        :string,  :default => nil
  config_param :account_id,        :string
  config_param :region,            :string
  config_param :s3_bucket,         :string
  config_param :s3_prefix,         :string,  :default => nil
  config_param :tag,               :string,  :default => 'elb.access_log'
  config_param :tsfile_path,       :string,  :default => '/var/tmp/fluent-plugin-elb-access-log.ts'
  config_param :histfile_path,     :string,  :default => '/var/tmp/fluent-plugin-elb-access-log.history'
  config_param :interval,          :time,    :default => 300
  config_param :start_datetime,    :string,  :default => nil
  config_param :buffer_sec,        :time,    :default => 600
  config_param :history_length,    :integer, :default => 100
  config_param :sampling_interval, :integer, :default => 1
  config_param :debug,             :bool,    :default => false

  def initialize
    super
    require 'csv'
    require 'fileutils'
    require 'logger'
    require 'time'
    require 'addressable/uri'
    require 'aws-sdk'
  end

  def configure(conf)
    super

    FileUtils.touch(@tsfile_path)
    FileUtils.touch(@histfile_path)
    tsfile_start_datetime = parse_tsfile

    if @start_datetime and not tsfile_start_datetime
      @start_datetime = Time.parse(@start_datetime).utc
    else
      if @start_datetime
        log.warn("start_datetime(#{@start_datetime}) is set. but tsfile datetime(#{tsfile_start_datetime}) is used")
      end

      @start_datetime = tsfile_start_datetime || Time.now.utc
    end

    @history = load_history
  end

  def start
    super

    # Load client
    client

    @loop = Coolio::Loop.new
    timestamp = @start_datetime

    timer = TimerWatcher.new(@interval, true, log) do
      new_timestamp = fetch(timestamp)

      if new_timestamp > timestamp
        save_timestamp(new_timestamp)
        timestamp = new_timestamp
      end

      if @history.length > @history_length
        @history.shift(@history.length - @history_length)
      end

      save_history
    end

    @loop.attach(timer)
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @loop.stop
    @thread.join
    super
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

          if logfile_suffix !~ /\.log\z/ or logfile_datetime <= (timestamp - @buffer_sec)
            next
          end

          unless @history.include?(obj.key)
            access_log = client.get_object(bucket: @s3_bucket, key: obj.key).body.string
            emit_access_log(access_log)
            last_timestamp = logfile_datetime
            @history.push(obj.key)
          end
        end
      end
    end

    last_timestamp
  end

  def prefixes(timestamp)
    base_prefix = "AWSLogs/#{@account_id}/elasticloadbalancing/#{@region}/"
    base_prefix = "#{@s3_prefix}/#{base_prefix}" if @s3_prefix

    [timestamp - 86400, timestamp, timestamp + 86400].map do |date|
      base_prefix + date.strftime('%Y/%m/%d/')
    end
  end

  def emit_access_log(access_log)
    if @sampling_interval > 1
      access_log = sampling(access_log)
    end

    parsed_access_log = []

    access_log.split("\n").each do |line|
      line = parse_line(line)
      parsed_access_log << line if line
    end

    parsed_access_log.each do |row|
      record = Hash[ACCESS_LOG_FIELDS.keys.zip(row)]

      ACCESS_LOG_FIELDS.each do |name, conv|
        record[name] = record[name].send(conv) if conv
      end

      split_address_port!(record, 'client')
      split_address_port!(record, 'backend')

      parse_request!(record)

      begin
        time = Time.parse(record['timestamp'])
        router.emit(@tag, time.to_i, record)
      rescue ArgumentError => e
        @log.warn("#{e.message}: #{row}")
        @log.warn('A record that has bad timestamp is not emitted.')
      end
    end
  end

  def parse_line(line)
    parsed = nil

    begin
      parsed = CSV.parse_line(line, :col_sep => ' ')
    rescue => e
      begin
        parsed = line.split(' ', 12)

        # request
        parsed[11] ||= ''
        parsed[11].sub!(/\A"/, '')
        parsed[11].sub!(/"(.*)\z/, '')

        user_agent, ssl_cipher, ssl_protocol = $1.strip.split(' ', 3)
        user_agent.sub!(/\A"/, '').sub!(/"\z/, '') if user_agent
        parsed[12] = user_agent
        parsed[13] = ssl_cipher
        parsed[14] = ssl_protocol
      rescue => e2
        @log.warn("#{e.message}: #{line}")
      end
    end

    parsed
  end

  def sampling(access_log)
    access_log.split("\n").each_with_index.select {|row, i| (i % @sampling_interval).zero? }.map {|row, i| row }.join("\n")
  end

  def split_address_port!(record, prefix)
    address_port = record["#{prefix}_port"]
    return unless address_port
    address, port = address_port.split(':', 2)
    record[prefix] = address
    record["#{prefix}_port"] = port.to_i
  end

  def parse_request!(record)
    request = record['request']
    return unless request
    method, uri, http_version = request.split(' ', 3)

    record['request.method'] = method
    record['request.uri'] = uri
    record['request.http_version'] = http_version

    begin
      uri = Addressable::URI.parse(uri)

      if uri
        [:scheme ,:user, :host, :port, :path, :query, :fragment].each do |key|
          record["request.uri.#{key}"] = uri.send(key)
        end
      end
    rescue => e
      @log.warn("#{e.message}: #{uri}")
    end
  end

  def save_timestamp(timestamp)
    open(@tsfile_path, 'w') do |tsfile|
      tsfile << timestamp.to_s
    end
  end

  def load_history
    File.read(@histfile_path).split("\n")
  end

  def save_history
    open(@histfile_path, 'w') do |histfile|
      histfile << @history.join("\n")
    end
  end

  def parse_tsfile
    Time.parse(File.read(@tsfile_path)).utc
  rescue
    nil
  end

  def client
    return @client if @client

    options = {:user_agent_suffix => USER_AGENT_SUFFIX}
    options[:region] = @region if @region
    options[:http_proxy] = @http_proxy if @http_proxy

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
