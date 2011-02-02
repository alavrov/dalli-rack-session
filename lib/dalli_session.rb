require 'rack/session/abstract/id'
require 'dalli'

module Rack
  module Session
    # Rack::Session::Dalli provides simple cookie based session management.
    # Session data is stored in memcached using dalli gem. The corresponding
    # session key is maintained in the cookie.
    # Based on Rack::Session:Memcache from rack

    class DalliSession < Abstract::ID
      attr_reader :mutex, :pool
      DEFAULT_OPTIONS = Abstract::ID::DEFAULT_OPTIONS.merge \
        :namespace => 'rack:session',
        :memcache_server => '127.0.0.1:11211'

      def initialize(app, options={})
        super

        @mutex = Mutex.new
        mserv = @default_options[:memcache_server]
        mopts = @default_options.
          reject{|k,v| !Dalli::Server::DEFAULTS.include? k }
        @pool = Dalli::Client.new mserv, mopts
        stat = @pool.stats
        if stat.empty? || stat.values.compact.empty?
          raise 'No memcache servers'
        end
      rescue Dalli::DalliError, Dalli::NetworkError, Dalli::RingError
        raise 'No memcache servers'
      end

      def generate_sid
        loop do
          sid = super
          break sid unless @pool.get(@default_options[:namespace]+":"+sid, :raw => true)
        end
      end

      def get_session(env, session_id)
        @mutex.lock if env['rack.multithread']
        unless session_id and session = @pool.get(@default_options[:namespace]+":"+session_id)
          session_id, session = generate_sid, {}
          unless @pool.add(@default_options[:namespace]+":"+session_id, session)
            raise "Session collision on '#{session_id.inspect}'"
          end
        end
        session.instance_variable_set '@old', @pool.get(@default_options[:namespace]+":"+session_id)# Dalli
        # can't perform raw get ATM, so we have to store old session as is.
        return [session_id, session]
      rescue Dalli::DalliError, Dalli::NetworkError, Dalli::RingError
        # MemCache server cannot be contacted
        warn "#{self} is unable to find memcached server."
        warn $!.inspect
        return [ nil, {} ]
      ensure
        @mutex.unlock if @mutex.locked?
      end

      def set_session(env, session_id, new_session, options)
        expiry = options[:expire_after]
        expiry = expiry.nil? ? 0 : expiry + 1

        @mutex.lock if env['rack.multithread']
        if options[:renew] or options[:drop]
          @pool.delete @default_options[:namespace]+":"+session_id
          return false if options[:drop]
          session_id = generate_sid
          @pool.add @default_options[:namespace]+":"+session_id, {} # so we don't worry about cache miss on #set
        end

        session = @pool.get(@default_options[:namespace]+":"+session_id) || {}
        old_session = new_session.instance_variable_get '@old'
        old_session = old_session ? old_session : {} # Dalli can't perform
        # raw get ATM, so we have to store old session as is.

        unless Hash === old_session and Hash === new_session
          env['rack.errors'].
            puts 'Bad old_session or new_session sessions provided.'
        else # merge sessions
          # alterations are either update or delete, making as few changes as
          # possible to prevent possible issues.

          # removed keys
          delete = old_session.keys - new_session.keys
          if $VERBOSE and not delete.empty?
            env['rack.errors'].
              puts "//@#{session_id}: delete #{delete*','}"
          end
          delete.each{|k| session.delete k }

          # added or altered keys
          update = new_session.keys.
            select{|k| new_session[k] != old_session[k] }
          if $VERBOSE and not update.empty?
            env['rack.errors'].puts "//@#{session_id}: update #{update*','}"
          end
          update.each{|k| session[k] = new_session[k] }
        end

        @pool.set @default_options[:namespace]+":"+session_id, session, expiry
        return session_id
      rescue Dalli::DalliError, Dalli::NetworkError, Dalli::RingError
        # MemCache server cannot be contacted
        warn "#{self} is unable to find memcached server."
        warn $!.inspect
        return false
      ensure
        @mutex.unlock if @mutex.locked?
      end
    end
  end
end
