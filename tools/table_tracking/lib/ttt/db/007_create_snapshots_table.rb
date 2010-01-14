require 'rubygems'
require 'active_record'
require 'ttt/db'
require 'ttt/table'
require 'ttt/table_volume'
require 'ttt/table_view'
require 'ttt/table_definition'
require 'set'

TTT::CollectorRegistry.load


class CreateSnapshotsTable < ActiveRecord::Migration
  def self.up
    create_table :snapshots do |t|
      t.integer :txn
      t.integer :collector_run_id
      t.integer :statistic_id
      t.integer :parent_txn
      t.timestamp :run_time
    end
    add_index :snapshots, :txn
    add_index :snapshots, :collector_run_id
    add_index :snapshots, :parent_txn, :name => 'snap_by_parent_txn'

    add_index :snapshots, [:txn, :collector_run_id], :name => 'snap_by_txn_and_collector'
    add_index :snapshots, [:statistic_id, :collector_run_id], :name => 'snap_by_stat_and_collector'
    add_index :snapshots, [:run_time, :collector_run_id], :name => 'snap_by_time_and_collector'

    TTT::TrackingTable.each do |c|
      next unless [:volume, :definition, :view].include? c.collector
      c_id=c.collector_id
      c.all(:select => :run_time, :group => :run_time, :order => :run_time).each do |defn_time|
        txn=TTT::Snapshot.head
        snap_ids=(TTT::Snapshot.find_all_by_txn(txn).collect { |tp| tp.statistic_id }).to_set
        c.all( :conditions => ['run_time = ?', defn_time.run_time]).each do |defn|
          TTT::Snapshot.create do |snap|
            snap.txn = txn+1
            snap.collector_run_id = c_id
            snap.statistic_id = defn.id
            snap.run_time = defn.run_time
            prev_defn=c.find(:last, :conditions => ['id < ? and server = ? and database_name = ? and table_name = ?', defn.id, defn.server, defn.database_name, defn.table_name ])
            unless prev_defn.nil?
              p_txn=TTT::Snapshot.find_last_by_collector_run_id_and_statistic_id(c_id, prev_defn.id)
              snap.parent_txn = p_txn.id
            end
          end
          snap_ids.delete defn.previous_version.id
        end
      end
    end
  end
  def self.down
    drop_table :snapshots
  end
end
