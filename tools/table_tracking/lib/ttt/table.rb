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
      def base.find_most_recent_versions(extra_params={})
        extra_copy = extra_params.clone
        find_params={
          :group => "server, database_name, table_name",
          :select => "MAX(id) AS max_id, *"
        }
        extra_copy.delete :select
        extra_copy.delete :group
        find_params.merge! extra_copy
        self.find(:all, find_params)
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
