require 'rubygems'
require 'active_record'
require 'ttt/collector'
require 'ttt/history'
require 'set'

module TTT

  # Mixin for TTT tracking tables.
  # Includes queries/methods that should be common to all.
  module TrackingTable
    @@tables = {}
    def self.tables
      @@tables
    end

    def self.each
      @@tables.each_value { |t| yield(t) }
    end

    def self.included(base)
      base.record_timestamps=false # Do no magic for tracking tables.
      base.class_inheritable_accessor :collector
      # Finds only the highest numbered id for each server.database.table
      # Returns them as an array of TableDefiniion objects.
      def base.find_most_recent_versions(extra_params={},txn=nil)
        c_id=TTT::CollectorRun.find_by_collector(self.collector.to_s).id
        latest_txn=nil
        begin
          latest_txn=TTT::Snapshot.find_last_by_collector_run_id(c_id).txn || TTT::Snapshot.head
        rescue NoMethodError
          latest_txn=TTT::Snapshot.head
        end
        if txn.class == Fixnum and txn < 0
          txn=latest-txn
          txn=0 if txn < 0
        elsif txn.class == Fixnum and txn > latest_txn
          txn=latest_txn
        elsif txn.nil? or txn.class != Fixnum
          txn=latest_txn
        end
        extra_copy = extra_params.clone
        find_params={
          :joins => %Q{INNER JOIN snapshots ON snapshots.collector_run_id=#{c_id} AND snapshots.txn=#{txn} AND #{self.table_name}.id=snapshots.statistic_id}
        }
        find_params.merge! extra_copy
        res=self.find(:all, find_params)
        res
      end

      def base.find_time_history(since=Time.now)
        c_id=TTT::CollectorRun.find_by_collector(self.collector.to_s).id
        #txns=TTT::Snapshot.all(:select => :txn, :conditions => ['run_time > ? AND collector_run_id = ?', since, c_id], :group => :txn).map { |s| s.txn }
        #self.find(:all, :joins => %Q{INNER JOIN snapshots ON snapshots.collector_run_id=#{c_id} AND snapshots.txn IN (#{txns.join(',')}) AND #{self.table_name}.id=snapshots.statistic_id})
        stats=TTT::Snapshot.all(:select => :statistic_id, :conditions => ['run_time > ? AND collector_run_id = ?', since, c_id]).map { |s| s.statistic_id }
        if stats
          self.find(stats.sort)
        else
          []
        end
      end

      def base.collector_id
        TTT::CollectorRun.find_by_collector(self.collector.to_s).id
      end

      def base.find_last_by_table(server, i_s_table)
        self.find_last_by_server_and_database_name_and_table_name(server, i_s_table.TABLE_SCHEMA, i_s_table.TABLE_NAME)
      end

      def base.last_run
        self.find(:last).run_time
      end

      def base.runs(over=nil)
        self.all(:select => :run_time, :group => :run_time, :conditions => (over.nil? ? [] : ['run_time > ?', over])).map { |r| r.run_time }
      end

      def base.servers
        TTT::Server.all.map { |f| f.name }
      end

      def base.schemas(server=:all)
        if server != :all
          TTT::Server.find_by_name(server).schemas.all
        else
          TTT::Schema.all
        end
      end

      def base.tables(server=:all, schema=:all)
        if server != :all and schema != :all
          TTT::Server.find_by_name(server).schemas.find_by_name(schema).tables.all
        elsif server != :all and schema == :all
          TTT::Server.find_by_name(server).tables.all
        elsif server == :all and schema != :all
          TTT::Schema.find_by_name(schema).tables.all
        else
          TTT::Table.all
        end

      end

      def base.collector=(sym)
        write_inheritable_attribute :collector, sym
        @@tables[sym]=self
      end

      def base.create_unreachable_entry(host,runtime)
        raise NotImplementedError, "This is an abstract method."
      end
    end

    def collector_id
      self.class.collector_id
    end

    def previous_version
      self.class.last(:conditions => ['id < ? AND server = ? AND database_name = ? AND table_name = ?', id, server, database_name, table_name])
    end

    def history(since=Time.at(0))
      self.class.all(:conditions => ['id <= ? AND run_time >= ? AND server = ? AND database_name = ? AND table_name = ?', id, since, server, database_name, table_name])
    end

    def unreachable?
      raise NotImplementedError, "This is an abstract method."
    end

    def deleted?
      raise NotImplementedError, "This is an abstract method."
    end

    def tchanged?
      raise NotImplementedError, "This is an abstract method."
    end

    def new?
      last=previous_version
      (self.history.length == 1) or (!last.nil? and last.deleted?)
    end

    def status
      if unreachable?
        :unreachable
      elsif deleted?
        :deleted
      elsif new?
        :new
      elsif tchanged?
        :changed
      else
        :unchanged
      end
    end


  end
end
