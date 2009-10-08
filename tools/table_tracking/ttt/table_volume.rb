require 'rubygems'
require 'activerecord'

module TTT
  class TableVolume < ActiveRecord::Base
    def self.aggregate_by_server(server)
    end
    def self.aggregate_by_schema(server,schema)
    end
    def self.aggregate_by_table(server,schema,table)
    end
  end
end
