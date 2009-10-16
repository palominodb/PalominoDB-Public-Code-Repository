require 'rubygems'
require 'activerecord'
require 'ttt/table'

module TTT
  class TableView < ActiveRecord::Base
    include TrackingTable
    self.collector= :view
    def unreachable?
      read_attribute(:database_name).nil? and read_attribute(:table_name).nil? and read_attribute(:create_syntax).nil?
    end
    def deleted?
      create_syntax().nil?
    end
    def tchanged?
      last=previous_version()
      last.nil? or last.deleted? or last.create_syntax() != create_synax()
    end
  end
end
