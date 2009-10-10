require 'rubygems'
require 'activerecord'
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
    
    # Finds only the 'x' highest numbered id(s) for each server.database.table
    # Returns them as an array of TableDefiniion objects.
    # TODO: Currently broken.
    def self.find_table_versions(x=:all,*args)
      selector=case args.first
      when :all, :first, :last
        args.shift
        args=args.first
      else
        unless args.empty?
          args=args.first
        else
          args={}
        end
        :all
      end
      unless x==:all
        args[:limit] = x
      end
      pp args
      pp selector
      pp x
      self.find(selector, args)
    end
  end
end
