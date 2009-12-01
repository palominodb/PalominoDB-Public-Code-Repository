require 'rubygems'
require 'active_record'
require 'ttt/table'

module TTT
  # Mapping to/from ttt.table_definitions table.
  # See ActiveRecord::Base and TTT::Table for more information.
  class TableDefinition < ActiveRecord::Base
    include TrackingTable
    self.collector= :definition
    def unreachable?
      read_attribute(:database_name).nil? and read_attribute(:table_name).nil? and read_attribute(:create_syntax).nil? and read_attribute(:created_at).nil? and read_attribute(:updated_at).nil?
    end

    def deleted?
      create_syntax().nil? and updated_at().nil?
    end

    def tchanged?
      last=previous_version()
      last.nil? or last.deleted? or last.created_at != created_at
    end

    def self.create_unreachable_entry(host,runtime)
      TTT::TableDefinition.new(
        :server => host,
        :database_name => nil,
        :table_name  => nil,
        :create_syntax => nil,
        :run_time => runtime,
        :created_at => "0000-00-00 00:00:00",
        :updated_at => "0000-00-00 00:00:00"
      )
    end

    def self.create_deleted_entry(host,runtime,schema,table)
    end
    
  end
end
