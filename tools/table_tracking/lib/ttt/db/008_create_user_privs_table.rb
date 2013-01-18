# 008_create_user_privs_table.rb
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

class CreateUserPrivsTable < ActiveRecord::Migration
  def self.up
    create_table :table_users do |tb|
      tb.integer :permtype,     :length => 2
      tb.string  :server,       :length => 100
      tb.string  :Host,         :length => 60
      tb.string  :Db,           :length => 64
      tb.string  :User,         :length => 16
      tb.string  :Table_name,   :length => 64
      tb.string  :Password,     :length => 41
      tb.string  :Column_name,  :length => 64
      tb.string  :Routine_name, :length => 64
      tb.string  :Routine_type, :length => 12
      tb.string  :Grantor,      :length => 92
      [ :Create_priv,
        :Drop_priv,
        :Grant_priv,
        :References_priv,
        :Event_priv,
        :Alter_priv,
        :Delete_priv,
        :Index_priv,
        :Insert_priv,
        :Select_priv,
        :Update_priv,
        :Create_tmp_table_priv,
        :Lock_tables_priv,
        :Trigger_priv,
        :Create_view_priv,
        :Show_view_priv,
        :Alter_routine_priv,
        :Create_routine_priv,
        :Execute_priv,
        :File_priv,
        :Create_user_priv,
        :Process_priv,
        :Reload_priv,
        :Repl_client_priv,
        :Repl_slave_priv,
        :Show_db_priv,
        :Shutdown_priv,
        :Super_priv
      ].each do |priv|
        tb.string priv, :limit => 1
      end
      tb.string :ssl_type, :limit => 20
      tb.binary :ssl_cipher
      tb.binary :x509_issuer
      tb.binary :x509_subject
      tb.integer :max_questions
      tb.integer :max_updates
      tb.integer :max_connections
      tb.integer :max_user_connections
      tb.timestamps
      tb.datetime :run_time
    end
  end

  def self.down
    drop_table :table_users
  end
end
