require 'rubygems'
require 'activerecord'

module TTT
  class TableDefinition < ActiveRecord::Base
    def self.find_most_recent_versions(server=nil)
      unless server.nil? then
        self.find(:all, :group => "server, database_name, table_name", :select => "MAX(id) AS id, server, database_name, table_name", :conditions => ["server = ?", server])
      else
        self.find(:all, :group => "server, database_name, table_name", :select => "MAX(id) AS id, server, database_name, table_name")
      end
    end
    
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
