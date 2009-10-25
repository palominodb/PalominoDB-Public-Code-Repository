require 'rubygems'
require 'activerecord'

class CreateCollectorRunTable < ActiveRecord::Migration
  def self.up
    create_table :collector_runs do |t|
      t.string :collector, :limit => 25
      t.timestamp :last_run
    end
    add_index :collector_runs, :collector, :unique => true
  end
  def self.down
    drop_table :collector_runs
  end
end
