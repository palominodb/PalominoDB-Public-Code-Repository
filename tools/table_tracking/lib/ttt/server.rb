# server.rb
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
require 'ttt/db'

module TTT
  class Server < ActiveRecord::Base
    has_many :schemas, :class_name => "TTT::Schema"
    has_many :tables, :through => :schemas, :class_name => "TTT::Table"
  end

  class Schema < ActiveRecord::Base
    set_table_name :server_schemas
    belongs_to :server, :class_name => "TTT::Server"
    has_many :tables, :class_name => "TTT::Table"
  end

  class Table < ActiveRecord::Base
    set_table_name :database_tables
    belongs_to :schema, :class_name => "TTT::Schema"

    def size
      cached_size
    end
  end
end
