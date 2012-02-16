require 'rubygems'
require 'active_record'

class CreateTableViewsTable < ActiveRecord::Migration
  def self.up
    create_table :table_views do |t|
      t.string :server, :limit => 100
      t.string :database_name, :limit => 64
      t.string :table_name, :limit => 64
      t.text :create_syntax
      t.timestamp :run_time
    end
  end

  def self.down
    drop_table :table_views
  end
end
