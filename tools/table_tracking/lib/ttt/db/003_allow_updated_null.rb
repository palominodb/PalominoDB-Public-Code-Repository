require 'rubygems'
require 'active_record'

class AllowUpdatedNull < ActiveRecord::Migration
  def self.up
      change_table :table_definitions do |t|
        t.change :updated_at, :timestamp, :null => true
      end
  end
  def self.down
      change_table :table_definitions do |t|
        t.change :updated_at, :timestamp, :null => false
      end
  end
end
