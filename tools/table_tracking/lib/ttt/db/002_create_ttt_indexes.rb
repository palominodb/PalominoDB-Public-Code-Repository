require 'rubygems'
require 'activerecord'

class CreateTttIndexes < ActiveRecord::Migration
  def self.up
    add_index :table_definitions, :run_time
    add_index :table_volumes, :run_time

    add_index :table_definitions, [:server, :database_name, :table_name ]
    add_index :table_volumes, [:server, :database_name, :table_name ]
  end
  def self.down
    remove_index :table_definitions, :run_time
    remove_index :table_volumes, :run_time

    remove_index :table_definitions, :column => [:server, :database_name, :table_name ]
    remove_index :table_volumes, :column => [:server, :database_name, :table_name ]
  end
end
