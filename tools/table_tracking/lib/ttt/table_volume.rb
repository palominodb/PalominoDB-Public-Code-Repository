require 'rubygems'
require 'activerecord'
require 'ttt/table'

module TTT
  # Mapping to/from ttt.table_volumes table.
  # See ActiveRecord::Base for more information.
  class TableVolume < ActiveRecord::Base
    include TrackingTable
    self.collector= :volume
    # Returns the most recent size (in bytes) for 'server'
    def self.aggregate_by_server(server)
      self.sum('bytes', :group => :server, :conditions => ['server = ? AND bytes NOT NULL', server])[server]
    end
    # Returns the most recent size (in bytes) for 'schema' on 'server'.
    def self.aggregate_by_schema(server,schema)
      self.sum('bytes', :group => 'server,database_name', :conditions => ['server = ? AND database_name = ? ', server, schema])[schema]
    end
    # Returns the most recent size (in bytes) for 'table' in 'schema' on 'server'.
    def self.aggregate_by_table(server,schema,table)
      self.sum('bytes', :group => 'server,database_name,table_name', :conditions => ['server = ? AND database_name = ? AND table_name = ?', server, schema, table])[table]
    end

    def unreachable?
      database_name().nil? and table_name().nil? and data_length().nil? and index_length().nil? and data_free().nil?
    end

    def deleted?
      data_length().nil? and index_length().nil? and data_free().nil?
    end

    def tchanged?
      true # This type always records information, even if it hasn't changed.
    end


  end
end
