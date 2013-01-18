# 005_create_collector_run_table.rb
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
