# table_definition.rb
# Copyright (C) 2009-2013 PalominoDB, Inc.
# 
# You may contact the maintainers at eng@palominodb.com.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

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
      last.nil? or last.deleted? or last.create_syntax() != create_syntax()
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
