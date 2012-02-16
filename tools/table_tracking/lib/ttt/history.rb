# Copyright (c) 2009-2010, PalominoDB, Inc.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
# 
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
# 
#   * Neither the name of PalominoDB, Inc. nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
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
