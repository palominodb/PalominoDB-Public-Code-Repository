require 'rubygems'
require 'activerecord'

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
