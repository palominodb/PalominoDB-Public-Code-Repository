require 'rubygems'
require 'active_record'
require 'ttt/collector'

class CreateCollectorRunTable < ActiveRecord::Migration
  def self.up
    create_table :collector_runs do |t|
      t.string :collector, :limit => 25
      t.timestamp :last_run
    end
    add_index :collector_runs, :collector, :unique => true
    TTT::CollectorRegistry.load
    TTT::CollectorRegistry.all.each do |c|
      TTT::CollectorRun.new(:collector => c.id.to_s).save!
    end
  end
  def self.down
    drop_table :collector_runs
  end
end
