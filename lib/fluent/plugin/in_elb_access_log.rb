require 'csv'
require 'fileutils'
require 'logger'
require 'time'
require 'addressable/uri'
require 'aws-sdk-s3'
require 'multiple_files_gzip_reader'

require 'fluent/input'
require 'fluent_plugin_elb_access_log/version'

class Fluent::Plugin::ElbAccessLogInput < Fluent::Input
  Fluent::Plugin.register_input('elb_access_log', self)

  USER_AGENT_SUFFIX = "fluent-plugin-elb-access-log/#{FluentPluginElbAccessLog::VERSION}"

  ACCESS_LOG_FIELDS = {
    # http://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html
    'clb' => {
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
    },
    # http://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html
    'alb' => {
      'type'                     => nil,
      'timestamp'                => nil,
      'elb'                      => nil,
      'client_port'              => nil,
      'target_port'              => nil,
      'request_processing_time'  => :to_f,
      'target_processing_time'   => :to_f,
      'response_processing_time' => :to_f,
      'elb_status_code'          => :to_i,
      'target_status_code'       => :to_i,
      'received_bytes'           => :to_i,
      'sent_bytes'               => :to_i,
      'request'                  => nil,
      'user_agent'               => nil,
      'ssl_cipher'               => nil,
      'ssl_protocol'             => nil,
      'target_group_arn'         => nil,
      'trace_id'                 => nil,
      'domain_name'              => nil,
      'chosen_cert_arn'          => nil,
    },
  }

  ELB_TYPES = %(clb alb)

  config_param :elb_type,          :string,  default: 'clb'
  config_param :aws_key_id,        :string,  default: nil, secret: true
  config_param :aws_sec_key,       :string,  default: nil, secret: true
  config_param :profile,           :string,  default: nil
  config_param :credentials_path,  :string,  default: nil
  config_param :http_proxy,        :string,  default: nil
  config_param :account_id,        :string
  config_param :region,            :string
  config_param :s3_bucket,         :string
  config_param :s3_prefix,         :string,  default: nil
  config_param :tag,               :string,  default: 'elb.access_log'
  config_param :tsfile_path,       :string,  default: '/var/tmp/fluent-plugin-elb-access-log.ts'
  config_param :histfile_path,     :string,  default: '/var/tmp/fluent-plugin-elb-access-log.history'
  config_param :interval,          :time,    default: 300
  config_param :start_datetime,    :string,  default: nil
  config_param :buffer_sec,        :time,    default: 600
  config_param :history_length,    :integer, default: 100
  config_param :sampling_interval, :integer, default: 1
  config_param :debug,             :bool,    default: false

  def configure(conf)
    super

    unless ELB_TYPES.include?(@elb_type)
      raise raise Fluent::ConfigError, "Invalid ELB type: #{@elb_type}"
    end

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
    @thread.kill
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
      client.list_objects(bucket: @s3_bucket, prefix: prefix).each do |page|
        page.contents.each do |obj|
          account_id, logfile_const, region, elb_name, logfile_datetime, ip, logfile_suffix = obj.key.split('_', 7)
          logfile_datetime = Time.parse(logfile_datetime)

          if logfile_suffix !~ /\.log(\.gz)?\z/ or logfile_datetime <= (timestamp - @buffer_sec)
            next
          end

          unless @history.include?(obj.key)
            access_log = client.get_object(bucket: @s3_bucket, key: obj.key).body

            if obj.key.end_with?('.gz')
              begin
                access_log = MultipleFilesGzipReader.new(access_log)

                # check gzip format
                access_log.first
                access_log.rewind
              rescue Zlib::Error => e
                @log.warn("#{e.message}: #{access_log.inspect.slice(0, 64)}")
                next
              end
            else
              access_log = access_log.each_line
            end

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

    records = parse_log(access_log)

    records.each do |record|
      begin
        time = Time.parse(record['timestamp'])
        router.emit(@tag, time.to_i, record)
      rescue ArgumentError => e
        @log.warn("#{e.message}: #{record}")
        @log.warn('A record that has bad timestamp is not emitted.')
      end
    end
  end

  def parse_log(access_log)
    parsed_access_log = []

    access_log.each do |line|
      line.chomp!

      case @elb_type
      when 'clb'
        line = parse_clb_line(line)
      when 'alb'
        line = parse_alb_line(line)
      end

      parsed_access_log << line if line
    end

    records = []
    access_log_fields = ACCESS_LOG_FIELDS.fetch(@elb_type)

    parsed_access_log.each do |row|
      record = Hash[access_log_fields.keys.zip(row)]

      access_log_fields.each do |name, conv|
        record[name] = record[name].send(conv) if conv
      end

      split_address_port!(record, 'client')

      case @elb_type
      when 'clb'
        split_address_port!(record, 'backend')
      when 'alb'
        split_address_port!(record, 'target')
      end

      parse_request!(record)

      records << record
    end

    records
  end

  def parse_clb_line(line)
    parsed = nil

    begin
      parsed = CSV.parse_line(line, col_sep: ' ')
    rescue => e
      begin
        parsed = line.split(' ', 12)

        # request
        parsed[11] ||= ''
        parsed[11].sub!(/\A"/, '')
        parsed[11].sub!(/"(.*)\z/, '')

        user_agent, ssl_cipher, ssl_protocol = rsplit($1.strip, ' ', 3)

        parsed[12] = unquote(user_agent)
        parsed[13] = ssl_cipher
        parsed[14] = ssl_protocol
      rescue => e2
        @log.warn("#{e.message}: #{e2.message}: #{line}")
      end
    end

    parsed
  end

  def parse_alb_line(line)
    parsed = nil

    begin
      parsed = CSV.parse_line(line, col_sep: ' ')
    rescue => e
      begin
        parsed = line.split(' ', 13)

        # request
        parsed[12] ||= ''
        parsed[12].sub!(/\A"/, '')
        parsed[12].sub!(/"(.*)\z/, '')

        user_agent, ssl_cipher, ssl_protocol, target_group_arn, trace_id, domain_name, chosen_cert_arn = rsplit($1.strip, ' ', 7)

        parsed[13] = unquote(user_agent)
        parsed[14] = ssl_cipher
        parsed[15] = ssl_protocol
        parsed[16] = target_group_arn
        parsed[17] = unquote(trace_id)
        parsed[18] = unquote(domain_name)
        parsed[19] = unquote(chosen_cert_arn)
      rescue => e2
        @log.warn("#{e.message}: #{e2.message}: #{line}")
      end
    end

    parsed
  end

  def sampling(access_log)
    access_log.each_with_index.select {|_, i| (i % @sampling_interval).zero? }.map(&:first)
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

    options = {user_agent_suffix: USER_AGENT_SUFFIX}
    options[:region] = @region if @region
    options[:http_proxy] = @http_proxy if @http_proxy

    if @aws_key_id and @aws_sec_key
      options[:access_key_id] = @aws_key_id
      options[:secret_access_key] = @aws_sec_key
    elsif @profile
      credentials_opts = {profile_name: @profile}
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

  def rsplit(str, sep, n)
    str = str.dup
    substrs = []

    (n - 1).times do
      pos = str.rindex(sep)
      next unless pos
      substr = str.slice!(pos..-1).slice(sep.length..-1)
      substrs << substr
    end

    substrs << str
    substrs.reverse
  end

  def unquote(str)
    return nil if (str || '').empty?
    str.sub(/\A"/, '').sub(/"\z/, '')
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
end # Fluent::Plugin::ElbAccessLogInput
