# history.rb
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

require 'ttt/collector'
require 'ttt/table'
module TTT
  class Snapshot < ActiveRecord::Base
    belongs_to :collector_run, :class_name => 'TTT::CollectorRun'
    def self.head(collector=nil)
      unless self.last.nil?
        self.last.txn
      else
        0
      end
    end
    def head
      self.find.last || 0
    end
    def self.txns()
      self.all(:select => :txn, :group => :txn).map { |s| s.txn }
    end
    def self.get(txn)
      self.find_all_by_txn(txn)
    end
    def self.get_next_txn()
      (self.head || -1)+1
    end
    def self.stat_is_new?(stat_obj)
      self.find_all_by_collector_run_id_and_statistic_id(stat_obj.collector_id, stat_obj.id).size == 1
    end

    # Override base transaction method
    # to automatically pass in a new 'transaction id'
    # to the block.
    def self.transaction()
      super() do
        yield(get_next_txn())
      end
    end
  end
end
