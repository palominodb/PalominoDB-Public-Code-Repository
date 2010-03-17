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
require 'rubygems'
require 'active_record'
require 'ttt/table'

module TTT
  # Mapping to/from ttt.table_definitions table.
  # See ActiveRecord::Base and TTT::Table for more information.
  class TableDefinition < ActiveRecord::Base
    include TrackingTable
    self.collector= :definition
    def unreachable?
      read_attribute(:database_name).nil? and read_attribute(:table_name).nil? and read_attribute(:create_syntax).nil? and read_attribute(:created_at).nil? and read_attribute(:updated_at).nil?
    end

    def deleted?
      create_syntax().nil? and updated_at().nil?
    end

    def tchanged?
      last=previous_version()
      last.nil? or last.deleted? or last.created_at != created_at
    end

    def self.create_unreachable_entry(host,runtime)
      TTT::TableDefinition.new(
        :server => host,
        :database_name => nil,
        :table_name  => nil,
        :create_syntax => nil,
        :run_time => runtime,
        :created_at => "0000-00-00 00:00:00",
        :updated_at => "0000-00-00 00:00:00"
      )
    end

    def self.create_deleted_entry(host,runtime,schema,table)
    end
    
  end
end
