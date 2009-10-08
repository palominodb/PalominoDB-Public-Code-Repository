require 'rubygems'
require 'activerecord'

module TTT
  # Mapping to/from ttt.table_volumes table.
  # See ActiveRecord::Base for more information.
  class TableVolume < ActiveRecord::Base
    # Returns the most recent size (in bytes) for 'server'
    def self.aggregate_by_server(server)
    end
    # Returns the most recent size (in bytes) for 'schema' on 'server'.
    def self.aggregate_by_schema(server,schema)
    end
    # Returns the most recent size (in bytes) for 'table' in 'schema' on 'server'.
    def self.aggregate_by_table(server,schema,table)
    end
  end
end
