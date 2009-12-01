require 'rubygems'
require 'active_record'
require 'ttt/table'

module TTT
  class TableView < ActiveRecord::Base
    include TrackingTable
    self.collector= :view
    def created_at
      run_time
    end
    def unreachable?
      read_attribute(:database_name).nil? and read_attribute(:table_name).nil? and read_attribute(:create_syntax).nil?
    end
    def deleted?
      create_syntax().nil?
    end
    def tchanged?
      last=previous_version()
      (last.nil? and TTT::Snapshot.stat_is_new? self) or !last.nil? and (last.deleted? or last.create_syntax() != create_syntax())
    end
    def self.create_unreachable_entry(host,runtime)
      self.new(
        :server => host,
        :database_name => nil,
        :table_name => nil,
        :create_syntax => nil,
        :run_time => runtime
      )
    end

  end
end
