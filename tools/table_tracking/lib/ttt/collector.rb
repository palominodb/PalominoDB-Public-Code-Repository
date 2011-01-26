# Copyright (c) 2009-2010, PalominoDB, Inc.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
# 
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
# 
#   * Neither the name of PalominoDB, Inc. nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
require 'rubygems'
require 'ttt/db'
require 'ttt/history'
require 'set'
require 'mysql'
require 'ttt/crash_reporter'

module TTT

  class CollectorRunningError < Exception; end

  module CollectorRegistry
    @@collectors=Set.new
    @@loaded=false

    def self.all
      @@collectors.to_a
    end

    def self.<<(o)
      register(o)
    end

    def self.register(obj)
      @@collectors<<obj
      @@collectors.to_a
    end
    # Forces a reload of all the collectors.
    # Useful for a long-running application (such as a web interface)
    def self.reload!
      self.unload
      self.load
    end
    # Loads all collectors under: <gems path>/table-tracking-toolkit-<version>/lib/ttt/collector/*
    # This must be called before collectors will function.
    def self.load(from=File.dirname(__FILE__)+"/collector/*")
      unless @@loaded
        Dir.glob( from ).each do |col|
          next if File.directory? col
          Kernel.load col
        end
        @@loaded=true
      end
    end

    # Forget all loaded collectors
    def self.unload
      @@collectors=[]
      @@loaded=false
    end
  end

  class CollectorRun < ActiveRecord::Base
    has_many :snapshots, :class_name => 'TTT::Snapshot'
    def self.find_by_collector(collector)
      if collector.class == String
        find_or_create_by_collector(collector)
      elsif collector.class == Symbol
        find_or_create_by_collector(collector.to_s)
      else
        find_or_create_by_collector(collector.stat.collector.to_s)
      end
    end
  end

  class CollectionDirector
    MYSQL_CONNECT_ERROR = Mysql::Error::CR_CONN_HOST_ERROR
    MYSQL_TOO_MANY_CONNECTIONS = Mysql::Error::ER_CON_COUNT_ERROR
    MYSQL_HOST_NOT_PRIVILEGED = Mysql::Error::ER_HOST_NOT_PRIVILEGED
    UNREACHABLE_ERRORS = [
                          MYSQL_CONNECT_ERROR,
                          MYSQL_TOO_MANY_CONNECTIONS,
                          MYSQL_HOST_NOT_PRIVILEGED,
                          Errno::EHOSTDOWN,
                          Errno::EHOSTUNREACH,
                          Errno::ECONNREFUSED,
                          Errno::ECONNABORTED,
                          Errno::ECONNRESET,
                          Errno::ETIMEDOUT,
                          Errno::ENETDOWN,
                          Errno::ENETUNREACH,
                          Errno::ENETRESET
                          ]
    class RunData
      attr_reader :host, :tables, :runref, :logger
      attr_accessor :cur_snapshot
      def initialize(host, tables, collector, runtime)
        @host=host
        @collector=collector
        @tables=tables
        @prev_snapshot=(collector.stat.find_most_recent_versions({:conditions => ['server = ?', host]}, :latest).collect { |v| v.id } ).to_set
        @cur_snapshot=@prev_snapshot.dup
        @runref=CollectorRun.find_by_collector(collector)
        @runref.last_run=runtime
        @logger=ActiveRecord::Base.logger
      end

      def this
        @collector
      end

      def stat
        @collector.stat
      end

      def runtime
        @runref.last_run
      end

      def get_prev_version
        stat.find(@prev_snapshot.to_a)
      end

      def stat_updated(new_id, old_id)
        self<<[new_id, old_id]
        delete(old_id)
      end

      def <<(ids)
        @cur_snapshot<<ids
      end

      def delete(_id)
        @cur_snapshot.delete _id
      end

      def snapshot
        @cur_snapshot
      end

      def changed?
        @prev_snapshot != @cur_snapshot
      end

      def save(txn)
        @runref.save
        save_run_ids(txn) # if changed? # be trusting for the time being.
      end
      private
      def save_run_ids(txn)
        ins_stmt=@runref.snapshots.connection.raw_connection.prepare(
          'INSERT INTO snapshots (collector_run_id, statistic_id, txn, parent_txn, run_time)
          VALUES(?, ?, ?, ?, ?)'
        )
        #changed=@cur_snapshot.select { |i| i.class == Array }
        #unchanged=@cur_snapshot.select { |i| i.class != Array }
        TTT::Snapshot.benchmark('Snapshot Save') do 
          @cur_snapshot.each do |i|
            @runref.snapshots.create do |snap|
              snap.txn = txn
              snap.run_time = @runref.last_run
              if i.class == Array
                snap.statistic_id = i[0]
                p_txn=@runref.snapshots.find_last_by_statistic_id(i[1])
                unless p_txn.nil?
                  snap.parent_txn = p_txn.id
                end
              else
                snap.statistic_id = i
              end
            end
          end
        end
      end
    end
    class TableCache < Array
      def initialize(*args)
        args.flatten!
        super(args)
        #self.reject! { |t| t.system_table? }
      end
      def find_by_schema_and_table(schema,table)
        (self.select { |t| t.schema==schema and t.name==table })[0]
      end
    end

    def initialize(cfg,runtime)
      @host=nil
      @runtime=runtime
      @cfg=cfg
      @cached_tables=nil
      CollectorRegistry.load # Make sure collectors are loaded.
    end

    def recache_tables!
      @cached_tables=TableCache.new(TTT::TABLE.all)
    end

    def collect(host, collector)
      CollectorRun.transaction do
        rd=nil
        if @host != host
          @host=host
          begin
            TTT::InformationSchema.connect(@host, @cfg)
            ActiveRecord::Base.logger.info "[cache tables]: #{@host} - #{collector.stat}"
            recache_tables!

            srv=TTT::Server.find_or_create_by_name(host)
            srv.updated_at = nil
            srv.save # Should reset updated_at.
            @cached_tables.each do |tbl|
              sch=srv.schemas.find_or_create_by_name(tbl.schema)
              sch.updated_at = nil
              sch.save # reset updated_at.
              t=sch.tables.find_or_create_by_name(tbl.name)
              t.updated_at = nil
              t.save
            end
          rescue Mysql::Error => mye
            if UNREACHABLE_ERRORS.include? mye.errno
              prev=collector.stat.find_last_by_server(@host)
              rd=RunData.new(host, nil, collector, @runtime)
              if prev.nil? or !prev.unreachable?
                rd.logger.info "[unreachable]: #{@host} - #{rd.stat}"
                t=collector.stat.create_unreachable_entry(@host, @runtime)
                t.save
                rd<<t.id
              end
              @host=nil # To force recheck for each stat.
            else
              raise mye
            end
          end

        end

        unless rd
          if(@cfg["ttt_connection"]["adapter"].downcase == "mysql")
            unless(CollectorRun.connection.select_value("SELECT GET_LOCK('ttt.collector.#{collector.stat.collector.to_s}',0.25)").to_i == 1)
              raise CollectorRunningError, "Only one collector per statistic may run at a time."
            end
          end # if mysql

          rd=RunData.new(@host, @cached_tables, collector, @runtime)
          collector.run(rd)

          if(@cfg["ttt_connection"]["adapter"].downcase == "mysql")
            CollectorRun.connection.execute("SELECT RELEASE_LOCK('ttt.collector.#{collector.stat.collector.to_s}')")
          end
        end
        rd
      end
    end # def collect

  end
  # Base class for all collectors.
  # A collector is actually a set of classes.
  # A class derived from ActiveRecord::Base (such as TableDefinition)
  # and a class derived from Collector (such as DefinitionCollector)
  # ttt-collect will then run each collector's 'collect' method
  # NOTE: There is no guaranteed order in which collectors are called.
  #       Do not depend on any particular order.
  # See: DefinitionCollector and VolumeCollector for examples.
  class Collector
    attr_reader :stat, :desc
    cattr_accessor :verbose
    # stat is a trackingtable constant
    # e.g., TTT::TableDefinition which this collector will use.
    def initialize(stat, desc, &actions)
      @stat = stat
      @desc = desc
      @actions = actions
      CollectorRegistry << self
    end

    def id
      stat.collector
    end

    def run(c_runner)
      c_runner.logger.info "[host-start] #{c_runner.host} - #{c_runner.stat}"
      res=@actions.call(c_runner)
      c_runner.logger.info "[host-end] #{c_runner.host} - #{c_runner.stat}"
      res
    end

    def eql? (o)
      o.stat == stat
    end

    def hash
      stat.hash
    end

    def to_yaml_properties
      %w{ @desc @action }
    end

  end

end
