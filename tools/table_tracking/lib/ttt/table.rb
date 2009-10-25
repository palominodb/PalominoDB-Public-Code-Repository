require 'rubygems'
require 'activerecord'

module TTT

  # Mixin for TTT tracking tables.
  # Includes queries/methods that should be common to all.
  module TrackingTable
    @@tables = {}
    def self.tables
      @@tables
    end

    def self.included(base)
      base.class_inheritable_accessor :collector
      # Finds only the highest numbered id for each server.database.table
      # Returns them as an array of TableDefiniion objects.
      def base.find_most_recent_versions(extra_params={},run_time=Time.now)
        self.connection.execute(
          %Q{CREATE TEMPORARY TABLE IF NOT EXISTS #{self.collector}_version_find (id INTEGER , run_time TIMESTAMP )}
          #%Q{CREATE TEMPORARY TABLE #{self.collector}_version_find SELECT MAX(id) as id, MAX(run_time) as run_time from table_volumes where run_time<'#{run_time}' group by server,database_name,table_name}
        )
        self.connection.execute(
          %Q{INSERT INTO #{self.collector}_version_find (id, run_time) SELECT MAX(id) as id, MAX(run_time) as run_time from table_volumes where table_volumes.run_time<'#{run_time}' group by server,database_name,table_name}
        )
        extra_copy = extra_params.clone
        find_params={
          :joins => %Q{INNER JOIN #{self.collector}_version_find USING(id)}
          #:group => "server, database_name, table_name",

          #:select => "*, MAX(id) AS max_id"
        }
        #extra_copy.delete :select
        #extra_copy.delete :group
        find_params.merge! extra_copy
        res=self.find(:all, find_params)
        self.connection.execute(%Q{TRUNCATE #{self.collector}_version_find})
        res
      end

      def base.last_run
        self.find(:last).run_time
      end

      def base.runs(over=nil)
        self.all(:select => :run_time, :group => :run_time, :conditions => (over.nil? ? [] : ['run_time > ?', over])).map { |r| r.run_time }
      end

      def base.servers
        self.find(:all, :group => [:server], :select => "server").map { |f| f.server }
      end

      def base.schemas(server=:all)
        self.find(:all, :group => "server, database_name", :select => "server, database_name", :conditions => server == :all ? [] : ['server = ?', server ] ).map { |f| s=self.new(:server => f.server, :database_name => f.database_name); s.readonly!; s }
      end

      def base.tables(server=:all, schema=:all)
        whr_str = ""
        conditions = []
        if server != :all
          whr_str += "server = ?"
          conditions << server
        end
        if schema != :all
          whr_str += ( whr_str == "" ? "database_name = ?" : " and database_name = ?" )
          conditions << schema
        end
        conditions << whr_str
        conditions.reverse!
        self.find(:all, :group => 'server, database_name, table_name', :select => "server, database_name, table_name", :conditions => conditions).map { |f| s=self.new(:server => f.server, :database_name => f.database_name, :table_name => f.table_name); s.readonly!; s }
      end

      def base.collector=(sym)
        write_inheritable_attribute :collector, sym
        @@tables[sym]=self
      end
    end

    def previous_version
      self.class.last(:conditions => ['id < ? AND server = ? AND database_name = ? AND table_name = ?', id, server, database_name, table_name])
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
      last=self.class.last(:conditions => ['id < ? AND server = ? AND database_name = ? AND table_name = ?', id, server, database_name, table_name])
      last.nil? or last.deleted?
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
