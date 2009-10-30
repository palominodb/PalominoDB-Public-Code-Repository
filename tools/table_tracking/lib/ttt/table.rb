require 'rubygems'
require 'activerecord'
require 'ttt/collector'
require 'ttt/history'

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
      base.class_inheritable_accessor :collector
      # Finds only the highest numbered id for each server.database.table
      # Returns them as an array of TableDefiniion objects.
      def base.find_most_recent_versions(extra_params={},txn=nil)
#        self.connection.execute(
#          %Q{CREATE TEMPORARY TABLE IF NOT EXISTS #{self.collector}_version_find (id INTEGER , run_time TIMESTAMP )}
          #%Q{CREATE TEMPORARY TABLE #{self.collector}_version_find SELECT MAX(id) as id, MAX(run_time) as run_time from table_volumes where run_time<'#{run_time}' group by server,database_name,table_name}
#        )
#        self.connection.execute(
#          %Q{INSERT INTO #{self.collector}_version_find (id, run_time) SELECT MAX(id) as id, MAX(run_time) as run_time from table_volumes where table_volumes.run_time<'#{run_time}' group by server,database_name,table_name}
#        )
        c_id=TTT::CollectorRun.find_by_collector(self.collector.to_s).id
        #txn=TTT::Snapshot.last_by_collector_run_id(c_id) || TTT::Snapshot.head if txn.nil? or txn.class != Fixnum
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
        #find_params={
        #  :joins => %Q{INNER JOIN #{self.collector}_version_find USING(id)}
        #  #:group => "server, database_name, table_name",

        #  #:select => "*, MAX(id) AS max_id"
        #}
        #extra_copy.delete :select
        #extra_copy.delete :group
        find_params.merge! extra_copy
        res=self.find(:all, find_params)
        #self.connection.execute(%Q{TRUNCATE #{self.collector}_version_find})
        res
      end

      def base.find_time_history(since=Time.now)
        c_id=TTT::CollectorRun.find_by_collector(self.collector.to_s).id
        txns=TTT::Snapshot.all(:select => :txn, :conditions => ['run_time > ? AND collector_run_id = ?', since, c_id], :group => :txn).map { |s| s.txn }
        self.find(:all, :joins => %Q{INNER JOIN snapshots ON snapshots.collector_run_id=#{c_id} AND snapshots.txn IN (#{txns.join(',')}) AND #{self.table_name}.id=snapshots.statistic_id})
      end

      def base.collector_id
        TTT::CollectorRun.find_by_collector(self.collector.to_s).id
      end

      def base.last_run
        self.find(:last).run_time
      end

      def base.runs(over=nil)
        self.all(:select => :run_time, :group => :run_time, :conditions => (over.nil? ? [] : ['run_time > ?', over])).map { |r| r.run_time }
      end

      def base.servers
        TTT::Server.all.map { |f| f.name }
        #self.find(:all, :group => [:server], :select => "server").map { |f| f.server }
      end

      def base.schemas(server=:all)
        if server != :all
          TTT::Server.find_by_name(server).schemas.all
        else
          TTT::Schema.all
        end
        #self.find(:all, :group => "server, database_name", :select => "server, database_name", :conditions => server == :all ? [] : ['server = ?', server ] ).map { |f| s=self.new(:server => f.server, :database_name => f.database_name); s.readonly!; s }
      end

      def base.tables(server=:all, schema=:all)
        if server != :all and schema != :all
          TTT::Server.find_by_name(server).schemas.find_by_name(schema).tables.all
        elsif server != :all and schema == :all
          TTT::Server.find_by_name(server).tables.all
        elsif server == :all and schema != :all
          TTT::Schema.find_by_name(schema).tablesa.all
        else
          TTT::Table.all
        end

        #whr_str = ""
        #conditions = []
        #if server != :all
        #  whr_str += "server = ?"
        #  conditions << server
        #end
        #if schema != :all
        #  whr_str += ( whr_str == "" ? "database_name = ?" : " and database_name = ?" )
        #  conditions << schema
        #end
        #conditions << whr_str
        #conditions.reverse!
        #self.find(:all, :group => 'server, database_name, table_name', :select => "server, database_name, table_name", :conditions => conditions).map { |f| s=self.new(:server => f.server, :database_name => f.database_name, :table_name => f.table_name); s.readonly!; s }
      end

      def base.collector=(sym)
        write_inheritable_attribute :collector, sym
        @@tables[sym]=self
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
      (self.history.empty?) or (!last.nil? and last.deleted?)
      #last=self.class.last(:conditions => ['id < ? AND server = ? AND database_name = ? AND table_name = ?', id, server, database_name, table_name])
      #if last.nil?
      #  TTT::Snapshot.find_all_by_statistic_id_and_collector_run_id(id,self.class.collector_id).empty?
      #else
      #  last.deleted?
      #end
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
