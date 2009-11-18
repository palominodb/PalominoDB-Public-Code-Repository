require 'rubygems'
require 'activerecord'
require 'ttt/table'

module TTT
  # Mapping to/from ttt.table_volumes table.
  # See ActiveRecord::Base for more information.
  class TableVolume < ActiveRecord::Base
    include TrackingTable
    self.collector= :volume

    after_create :update_cached_table_size

    # Returns the most recent size (in bytes) for 'server'
    def self.aggregate_by_server(server)
      self.server_size(server)
    end

    # Returns the most recent size (in bytes) for 'server'
    def self.server_size(server)
      s=self.server_sizes(server)
      if s.data_length.nil? or s.index_length.nil?
        nil
      else
        s.size
      end
    end
    def self.server_sizes(server, time=:latest)
      r=nil
      if time==:latest
        r=find_most_recent_versions(:conditions => ["server = ?", server])
      else
        r=find_most_recent_versions({:conditions => ["server = ?", server]}, time)
      end
      d_length=0
      i_length=0
      r.each do |t|
        next if t.unreachable?
        d_length+=t.data_length
        i_length+=t.index_length
      end
      v=self.new(:server => server, :data_length => d_length == 0 ? nil : d_length, :index_length => i_length == 0 ? nil : i_length, :run_time => r[-1].nil? ? nil : r[-1].run_time)
      v.readonly!
      v
    end
    # Returns the most recent size (in bytes) for 'schema' on 'server'.
    def self.aggregate_by_schema(server,schema)
      self.database_size
    end
    # Returns the most recent size (in bytes) for 'schema' on 'server'.
    def self.database_size(server,database)
      self.database_sizes(server,database).size
    end
    # Returns the most recent size (in bytes) for 'schema' on 'server'.
    def self.database_sizes(server,database,time=:latest)
      r=nil
      if time==:latest
        r=find_most_recent_versions(:conditions => ["server = ? and database_name = ?", server, database])
      else
        r=find_most_recent_versions({:conditions => ["server = ? and database_name = ?", server, database]}, time)
      end

      d_length=0
      i_length=0
      r.each do |t|
        next if t.unreachable?
        d_length+=t.data_length
        i_length+=t.index_length
      end
      v=self.new(:server => server, :database_name => database, :data_length => d_length == 0 ? nil : d_length, :index_length => i_length == 0 ? nil : i_length, :run_time => r[-1].run_time)
      v.readonly!
      v
    end
    ## Returns the most recent size (in bytes) for 'table' in 'schema' on 'server'.
    #def self.aggregate_by_table(server,schema,table)
    #  r=find_most_recent_versions(:conditions => ["server = ? and database_name = ? and table_name = ?", server, schema, table])
    #  size=0
    #  r.each do |t|
    #    size+= t.size # N.B. Data free not included, may not return the number you expect.
    #  end
    #  size
    #end

    def unreachable?
      database_name().nil? and table_name().nil? and data_length().nil? and index_length().nil? and data_free().nil?
    end

    def deleted?
      data_length().nil? and index_length().nil? and data_free().nil?
    end

    def tchanged?
      prev=previous_version
      if prev.nil?
        true
      else
        data_length != prev.data_length || index_length != prev.index_length
      end
    end

    def size
      unless data_length.nil? or index_length.nil?
        data_length+index_length
      else
        nil
      end
    end

    def self.create_unreachable_entry(host, runtime)
      self.new(
        :server => host,
        :database_name => nil,
        :table_name => nil,
        :data_free => nil,
        :index_length => nil,
        :data_length => nil,
        :run_time => runtime
      )
    end

    private
    def update_cached_table_size
      s=TTT::Server.find_by_name(server)
      sch=s.schemas.find_by_name(database_name)
      unless sch.nil?
        t=sch.tables.find_by_name(table_name)
        t.cached_size=size
        t.save
      end
    end

  end
end
