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
    def self.get(txn=nil)
    end

    def self.stat_is_new?(stat_obj)
      self.find_all_by_collector_run_id_and_statistic_id(stat_obj.collector_id, stat_obj.id).size == 1
    end
  end
end
