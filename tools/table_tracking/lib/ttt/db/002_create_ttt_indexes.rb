# 002_create_ttt_indexes.rb
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

class CreateTttIndexes < ActiveRecord::Migration
  def self.up
    add_index :table_definitions, :run_time
    add_index :table_volumes, :run_time

    add_index :table_definitions, [:server, :database_name, :table_name ], :name => 'defn_by_server_schema_table'
    add_index :table_volumes, [:server, :database_name, :table_name ], :name => 'volu_by_server_schema_table'
  end
  def self.down
    remove_index :table_definitions, :run_time
    remove_index :table_volumes, :run_time

    remove_index :table_definitions, :column => [:server, :database_name, :table_name ], :name => 'by_server_schema_table'
    remove_index :table_volumes, :column => [:server, :database_name, :table_name ], :name => 'by_server_schema_table'
  end
end
