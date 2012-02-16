require 'rubygems'
require 'active_record'

class CreateTttBaseTables < ActiveRecord::Migration
  def self.up
    say "Creating table schema table.."
    create_table :table_definitions do |t|
      t.string :server, :limit => 100
      t.string :database_name, :limit => 64
      t.string :table_name, :limit => 64
      t.text :create_syntax
      t.timestamp :run_time
      t.timestamps
    end

    say "Creating table volume table.."
    create_table :table_volumes do |t|
      t.string :server, :limit => 100
      t.string :database_name, :limit => 64
      t.string :table_name, :limit => 64
      t.integer :data_length, :limit => 8
      t.integer :index_length, :limit => 8
      t.integer :data_free, :limit => 8
      t.timestamp :run_time
    end
    #add_index :table_definitions, [:server, :database, :table]
    #add_index :table_definitions, [:created_at, :updated_at]
  end
  def self.down
    drop_table :table_definitions
    drop_table :table_volumes
  end
end # CreateBaseTables
