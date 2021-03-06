# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "redis"
require "redlock"
require "monitor"
require "json"

class LogStash::Filters::CacheRedis < LogStash::Filters::Base
    config_name "cache_redis"


    config :operate, :validate => :string, :default => "get"

    config :redis_key, :validate => :string, :default => "%{_id}"

    config :redis_val, :validate => :string, :default => "%{_message}"

    config :field, :validate => :string, :default => "message"


    config :wait_interval, :validate => :number, :default => 0

    config :wait_max_time, :validate => :number, :default => 0

    config :expire_ex, :validate => :number, :default => 0

    config :tag_on_failure, :validate => :string, :default => "_cache_redis_failure"

    config :remain_origin, :validate => :boolean, :default => "false"

    config :remain_fields, :validate => :array, :default => ["@timestamp", "[@timestamp]"]

    config :ignore_fields, :validate => :array, :default => []




    # The hostname(s) of your Redis server(s). Ports may be specified on any
    # hostname, which will override the global port config.
    # If the hosts list is an array, Logstash will pick one random host to connect to,
    # if that host is disconnected it will then pick another.
    #
    # For example:
    # [source,ruby]
    #     "127.0.0.1"
    #     ["127.0.0.1", "127.0.0.2"]
    #     ["127.0.0.1:6380", "127.0.0.1"]
    config :host, :validate => :array, :default => ["127.0.0.1"]

    # Shuffle the host list during Logstash startup.
    config :shuffle_hosts, :validate => :boolean, :default => true

    # The default port to connect on. Can be overridden on any hostname.
    config :port, :validate => :number, :default => 6379

    # The Redis database number.
    config :db, :validate => :number, :default => 0

    # Redis initial connection timeout in seconds.
    config :timeout, :validate => :number, :default => 5

    # Password to authenticate with.  There is no authentication by default.
    config :password, :validate => :password

    # Interval for reconnecting to failed Redis connections
    config :reconnect_interval, :validate => :number, :default => 0.2

    # Maximal count of command retries after a crash because of a failure
    config :max_retries, :validate => :number, :default => 3

    # Interval for retrying to acquire a lock
    config :lock_retry_interval, :validate => :number, :default => 1

    # Maximal count of retries to acquire a lock
    config :max_lock_retries, :validate => :number, :default => 3

    # config :get, :validate => :boolean, :default => false
    config :lock_timeout, :validate => :number, :default => 5000






    public
    def register
        @redis = nil
        @mul_redis = nil
        @lock_manager = nil
        if @shuffle_hosts
            @host.shuffle!
        end
        @host_idx = 0

        @lock = Monitor.new


        @CMD_GET = "get"
        @CMD_SET = "set"
        @CMD_GETDEL = "getdel"
        @CMD_SETNX = "setnx"
        @CMD_DEL = "del"
        @CMD_CACHE_EVENT = "cache_evnet"
        @CMD_USE_EVENT = "use_event"
    end # def register


    def filter(event)

        # TODO: Maybe refactor the interface into a more flexible one with two
        #       main configs 'cmd' & 'args'. Then it would be possible to eliminate
        #       all if clauses and replace it through one hashmap call, where
        #       the hashmap would be a mapping from 'cmd' -> <cmd_function_ref>
        #       E.q.: cmds.fetch(event.get(@llen), &method(:cmd_not_found_err))
        max_retries = @max_retries
        begin
            @redis ||= connect


            l_wait_max_time = @wait_max_time
            cmd_res = true

            if @operate.nil?
                @logger.error("no operate declared !", :event => event,)

            else

                if @operate.eql?(@CMD_SET)
                    if @expire_ex > 0
                        cmd_res = @redis.set(event.sprintf(@redis_key), event.sprintf(@redis_val), ex:@expire_ex)
                    else
                        cmd_res = @redis.set(event.sprintf(@redis_key), event.sprintf(@redis_val))
                    end

                elsif @operate.eql?(@CMD_GET)
                    val = @redis.get(event.sprintf(@redis_key))
                    while val.nil? and l_wait_max_time > 0 do
                        sleep(@wait_interval)
                        val = @redis.get(event.sprintf(@redis_key))
                        l_wait_max_time = l_wait_max_time - 1
                    end
                    event.set(@field, val)

                    if val.nil?
                        cmd_res = false;
                    end

                elsif @operate.eql?(@CMD_GETDEL)
                    val = @redis.get(event.sprintf(@redis_key))
                    while val.nil? and l_wait_max_time > 0 do
                        sleep(@wait_interval)
                        val = @redis.get(event.sprintf(@redis_key))
                        l_wait_max_time = l_wait_max_time - 1
                    end
                    event.set(@field, val)

                    if val.nil?
                        cmd_res = false;
                    else
                        @redis.del(event.sprintf(@redis_key))
                    end

                elsif @operate.eql?(@CMD_SETNX)
                    if @expire_ex > 0
                        cmd_res = @redis.set(event.sprintf(@redis_key), event.sprintf(@redis_val), ex:@expire_ex, nx:true)
                    else
                        cmd_res = @redis.set(event.sprintf(@redis_key), event.sprintf(@redis_val), nx:true)
                    end

                elsif @operate.eql?(@CMD_DEL)
                    cmd_res = @redis.del(event.sprintf(@redis_key))

                elsif @operate.eql?(@CMD_CACHE_EVENT)


                    fields = event.to_hash.keys.map { |k| "[#{k}]" }

                    m_r = nil
                    c_ffs = Array.new

                    begin
                        @lock.synchronize do
                            @mul_redis ||= connect
                            @mul_redis.multi()
                            fields.each do |ffield|
                                redis_cache_hash_field(event, @redis_key, ffield, @ignore_fields, c_ffs)
                            end
                            if @expire_ex > 0
                                @mul_redis.expire(event.sprintf(@redis_key), @expire_ex)
                                c_ffs << "expire command"
                            end
                            m_r = @mul_redis.exec()
                        end

                    rescue => e

                        begin
                            @mul_redis.discard()
                        rescue => ee
                            @logger.warn("redis discard() failed!", event => event)
                        end
                        max_retries -= 1
                        unless max_retries < 0
                            sleep @reconnect_interval
                            retry
                        else
                            @logger.error("Max retries reached for trying to execute a command",
                              :event => event, :exception => e)
                            event.tag(@tag_on_failure)
                        end
                    end

                    if m_r.nil?
                        cmd_res = false
                        @logger.warn("redis_cache_event failed, redis.exec() not running", event => event)
                    else
                        ii = 0;
                        m_r.each do |ff|
                            if ff != 1
                                cmd_res = false
                                ftmp = c_ffs[ii]
                                @logger.warn("redis.multi() queue failed [#{ftmp}] : #{ff} ...", :event => event)
                            end
                            ii = ii + 1
                        end
                    end



                elsif @operate.eql?(@CMD_USE_EVENT)
                    n_event = @redis.hgetall(event.sprintf(@redis_key))
                    while n_event.empty? and l_wait_max_time > 0 do
                        sleep(@wait_interval)
                        n_event = @redis.hgetall(event.sprintf(@redis_key))
                        l_wait_max_time = l_wait_max_time - 1
                    end

                    if n_event.empty?
                        cmd_res = false
                        @logger.warn("redis cache, use_event recieve empty event ...", :event => event)
                    else
                        if not @remain_origin
                            origin_fields = event.to_hash.keys.map { |k| "[#{k}]" }
                            origin_fields.each do |ori_f|
                                remove_field(event, ori_f, @remain_fields)
                            end
                        end


                        fields = n_event.keys
                        fields.each do |ffield|
                            begin
                                resolve_val = redis_resolve_cache_field(n_event[ffield], event, ffield)
                                event.set(ffield, resolve_val)
#                                 deserialize_val = Marshal.load(n_event[ffield])
#                                 event.set(ffield, deserialize_val)
                            rescue => e
                                @logger.error("use_event, set event field failed. ", :event => event, :exception => e, :backtrace => e.backtrace)
                            end
#                             if ffield == "@timestamp" || ffield == "[@timestamp]"
#                                 val = LogStash::Timestamp.new(n_event[ffield])
#                                 event.set(ffield, val)
#                             else
#                                 event.set(ffield, n_event[ffield])
#                             end
                        end

                    end






                end



                if not cmd_res
                    event.tag(@tag_on_failure)
                end

            end





        rescue => e
            @logger.warn("Failed to send event to Redis, retrying after #{@reconnect_interval} seconds...", :event => event,
                         :exception => e, :backtrace => e.backtrace)
            sleep @reconnect_interval
            @redis = nil
            @lock_manager = nil
            max_retries -= 1
            unless max_retries < 0
                retry
            else
                @logger.error("Max retries reached for trying to execute a command",
                              :event => event, :exception => e)
                event.tag(@tag_on_failure)
            end
        end

        # filter_matched should go in the last line of our successful code
        filter_matched(event)
    end # def filter


    private
    def connect
        @current_host, @current_port = @host[@host_idx].split(':')
        @host_idx = @host_idx + 1 >= @host.length ? 0 : @host_idx + 1

        if not @current_port
            @current_port = @port
        end

        params = {
            :host => @current_host,
            :port => @current_port,
            :timeout => @timeout,
            :db => @db
        }
        @logger.debug("connection params", params)

        if @password
            params[:password] = @password.value
        end

        Redis.new(params)
    end # def connect

    def connect_lockmanager
        hosts = Array(@host).map { |host|
            host.prepend('redis://') unless host.start_with?('redis://')
        }
        @logger.debug("lock_manager hosts", hosts)

        Redlock::Client.new(hosts)
    end # def connect



    def redis_cache_hash_field(event, redis_key, ff, ignore_ff, arr_fields)
        val = event.get(ff)
        if val.is_a?(Hash) || val.is_a?(java.util.Map)
            val.keys.each do |key|
                redis_cache_hash_field(event, redis_key, "#{ff}[#{key}]", ignore_ff, arr_fields)
            end
        else
            if not ignore_ff.include?(ff)
                begin
                    val_clz = val.class.to_s
                    tmp_hash = { :clz => val_clz, :val => val}
                    val_json_str = JSON.generate(tmp_hash)
                    @mul_redis.hset(event.sprintf(redis_key), ff, val_json_str)
                rescue => e
                    @logger.error("cache_event.cache_field [#{ff} => #{val}] failed!  ", :event => event, :exception => e, :backtrace => e.backtrace)
                    @mul_redis.hset(event.sprintf(redis_key), ff, val)
                end
                arr_fields << ff
            end
        end

    end

    def redis_resolve_cache_field(json_str, event, field)
        ret_val = nil
        begin
            tmp_hash = JSON.parse(json_str)
            if tmp_hash["clz"] == "LogStash::Timestamp"
                ret_val = LogStash::Timestamp.new(tmp_hash["val"])
            else
                ret_val = tmp_hash["val"]
            end
        rescue => e
            @logger.error("use_event.use_field [#{field}] failed!  ", :event => event, :exception => e, :backtrace => e.backtrace)
            ret_val = json_str
        end

        ret_val
    end


    def remove_field(event, ff, remains)
        val = event.get(ff)
        if val.is_a?(Hash) || val.is_a?(java.util.Map)
            val.keys.each do |key|
                remove_field(event, "#{ff}[#{key}]", remains)
            end
        else
            if not remains.include?(ff)
                event.remove(ff)
            end
        end

    end

end # class LogStash::Filters::Example
