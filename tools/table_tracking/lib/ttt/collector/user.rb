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
require 'ttt/db'
require 'ttt/table_user'
require 'ttt/server'
require 'ttt/formatters'

TTT::Collector.new(TTT::TableUser, "user privilige tracking") do |rd|
  mysqlusers   = TTT::Db.open_schema(rd.host, 'mysql', 'user')
  mysqlhosts   = TTT::Db.open_schema(rd.host, 'mysql', 'host')
  mysqldbs     = TTT::Db.open_schema(rd.host, 'mysql', 'db')
  mysqltables  = TTT::Db.open_schema(rd.host, 'mysql', 'tables_priv')
  mysqlcolumns = TTT::Db.open_schema(rd.host, 'mysql', 'columns_priv')
  mysqlprocs   = TTT::Db.open_schema(rd.host, 'mysql', 'procs_priv')

  prev_version=rd.get_prev_version

  # Global privileges
  mysqlusers.all.each do |mu|
    pu = prev_version.select { |e| e.global_perm? and e.User == mu.User and e.Host == mu.Host }[0]
    next unless pu.nil?

    s=rd.stat.new(mu.attributes.merge(:server => rd.host, :created_at => rd.runtime, :updated_at => rd.runtime, :run_time => rd.runtime))
    s.permtype = TTT::TableUser::GLOBAL_PERMISSION
    rd.logger.debug "[new global user priv]: '#{s.User}'@'#{s.Host}'"
    s.save
    rd<<s.id
  end

  # Host level privs
  mysqlhosts.all.each do |mu|
    pu = prev_version.select { |e| e.host_perm? and e.Host == mu.Host and e.Db == mu.Db }[0]
    next unless pu.nil?

    a=mu.attributes.merge(:server => rd.host, :created_at => rd.runtime, :updated_at => rd.runtime, :run_time => rd.runtime)
    s=rd.stat.new(a)
    s.permtype = TTT::TableUser::HOST_PERMISSION
    rd.logger.debug "[new host user priv]: *.* from '#{s.User}'@'#{s.Host}'"
    s.save
    rd<<s.id
  end

  # DB level privs
  mysqldbs.all.each do |mu|
    pu = prev_version.select { |e| e.db_perm? and e.User == mu.User and e.Host == mu.Host and e.Db == mu.Db }[0]
    next unless pu.nil?

    a=mu.attributes.merge(:server => rd.host, :created_at => mu['Timestamp'], :updated_at => rd.runtime, :run_time => rd.runtime)
    a.delete 'Timestamp'
    s=rd.stat.new(a)
    s.permtype = TTT::TableUser::DB_PERMISSION
    rd.logger.debug "[new db user priv]: `#{s.Db}`.* from '#{s.User}'@'#{s.Host}'"
    s.save
    rd<<s.id
  end

  # Table level privs
  mysqltables.connection.select_all('SELECT * FROM mysql.tables_priv').each do |mu|
    pu = prev_version.select { |e| e.table_perm? and e.User == mu['User'] and e.Host == mu['Host'] and e.Db == mu['Db'] and e.Table_name == mu['Table_name'] }[0]
    next unless pu.nil?

    a=mu.merge(:server => rd.host, :created_at => mu['Timestamp'], :updated_at => rd.runtime, :run_time => rd.runtime)
    a.delete 'Timestamp'
    # Column_priv masks the various column level privileges that are applied to a table
    # MySQL uses it to determine if it should look in column_priv. We currently don't track this
    # since we're tracking column_priv table anyway.
    # And while tracking it could be used to sniff out corruption or perhaps privilege abuse
    # it doesn't seem worthwhile straight away.
    a.delete 'Column_priv'
    tprivs=TTT::TableUser.perms_from_setstr(a['Table_priv'])
    a.delete 'Table_priv'
    s=rd.stat.new(a)
    s.perms_from_set(tprivs)
    s.permtype = TTT::TableUser::TABLE_PERMISSION
    rd.logger.debug "[new table user priv]: `#{s.Db}`.`#{s.Table_name}` from '#{s.User}'@'#{s.Host}'"
    s.save
    rd<<s.id
  end

  # Column level privs
  mysqlcolumns.all.each do |mu|
    pu = prev_version.select { |e| e.column_perm? and e.User == mu.User and e.Host == mu.Host and e.Db == mu.Db and e.Table_name == mu.Table_name and e.Column_name == mu.Column_name }[0]
    next unless pu.nil?

    a=mu.attributes.merge(:server => rd.host, :created_at => mu['Timestamp'], :updated_at => rd.runtime, :run_time => rd.runtime)
    tprivs=TTT::TableUser.perms_from_setstr(mu.Column_priv_before_type_cast)
    a.delete 'Timestamp'
    a.delete 'Column_priv'
    s=rd.stat.new(a)
    s.perms_from_set(tprivs)
    s.permtype = TTT::TableUser::COLUMN_PERMISSION
    rd.logger.debug "[new column user priv]: `#{s.Db}`.`#{s.Table_name}`.`#{s.Column_name}` from '#{s.User}'@'#{s.Host}'"
    s.save
    rd<<s.id
  end

  # Proc level privs
  mysqlprocs.all.each do |mu|
    pu = prev_version.select { |e| e.proc_perm? and e.User == mu.User and e.Host == mu.Host and e.Db == mu.Db and e.Routine_name == mu.Routine_name and e.Routine_type == mu.Routine_type }[0]
    next unless pu.nil?

    a=mu.attributes.merge(:server => rd.host, :created_at => mu['Timestamp'], :updated_at => rd.runtime, :run_time => rd.runtime)
    tprivs=TTT::TableUser.perms_from_setstr(mu.Proc_priv_before_type_cast)
    a.delete 'Timestamp'
    a.delete 'Proc_priv'
    s=rd.stat.new(a)
    s.perms_from_set(tprivs)
    s.permtype = TTT::TableUser::PROC_PERMISSION
    rd.logger.debug "[new proc priv]: `#{s.Db}`.`#{s.Table_name}`.`#{s.Column_name}` from '#{s.User}'@'#{s.Host}'"
    s.save
    rd<<s.id
  end
  
  # Global privs
  prev_version.each do |u|
    curp = 
      case u.permtype & ~0x3
      when TTT::TableUser::GLOBAL_PERMISSION
        mysqlusers.find(:first,:conditions => ['User = ? AND Host = ?', u.User, u.Host])
      when TTT::TableUser::HOST_PERMISSION
        mysqlhosts.find(:first,:conditions => ['Host = ? AND Db = ?', u.Host, u.schema])
      when TTT::TableUser::DB_PERMISSION
        mysqldbs.find(:first,:conditions => ['Host = ? AND Db = ? AND User = ?', u.Host, u.schema, u.User])
      when TTT::TableUser::TABLE_PERMISSION
        mysqltables.find(:first, :conditions => ['Host = ? AND Db = ? AND User = ? AND Table_name = ?', u.Host, u.schema, u.User, u.Table_name])
      when TTT::TableUser::COLUMN_PERMISSION
        mysqlcolumns.find(:first, :conditions => ['Host = ? AND Db = ? AND User = ? AND Table_name = ? AND Column_name = ?', u.Host, u.schema, u.User, u.Table_name, u.Column_name])
      when TTT::TableUser::PROC_PERMISSION
        mysqlprocs.find(:first, :conditions => ['Host = ? AND Db = ? AND User = ? AND Routine_name = ? AND Routine_type = ?', u.Host, u.schema, u.User, u.Routine_name, u.Routine_type])
      when TTT::TableUser::UNREACHABLE_ENTRY
        next
      else
        raise RuntimeError, "Invalid, Corrupt, or hand modified data found. Found user was not of any known type."
        nil
      end
    next if u.deleted? and curp.nil?
    # Delete check
    if curp.nil? and !u.deleted?
      newu = rd.stat.new(u.attributes.merge(:created_at => nil, :updated_at => nil, :run_time => rd.runtime, :permtype => u.permtype | TTT::TableUser::DELETED_PERMISSION))
      newu.save
      rd.stat_updated(newu.id, u.id)
      next
    end
    changed=false
    [ :Password,
      :Grantor,
      :Create_priv,
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

      # The set colums are special because activerecord sees them as
      # timestamps, which is WRONG, we have to access them un-typecast
      # and interpret.
      :Proc_priv,   # Set column in procs_priv
      :Table_priv,  # Set column in tables_priv
      :Column_priv, # Set column in columns_priv

      :File_priv,
      :Create_user_priv,
      :Process_priv,
      :Reload_priv,
      :Repl_client_priv,
      :Repl_slave_priv,
      :Show_db_priv,
      :Shutdown_priv,
      :Super_priv,
      :ssl_type,
      :ssl_cipher,
      :x509_issuer,
      :x509_subject,
      :max_questions,
      :max_updates,
      :max_connections,
      :max_user_connections
    ].each do |priv|
      next unless curp.has_attribute? priv
      curp_val = curp[priv]
      u_val    = u[priv]
      if priv == :Proc_priv and u.proc_perm?
        curp_val = Set.new(curp.Proc_priv_before_type_cast.split(/,/).map { |pr| (pr.gsub(" ", '_') + "_priv").capitalize.to_sym })
        u_val    = u.perms_set
      elsif priv == :Table_priv and u.table_perm?
        curp_val = Set.new(curp.Table_priv_before_type_cast.split(/,/).map { |pr| (pr.gsub(" ", '_') + "_priv").capitalize.to_sym })
        u_val    = u.perms_set
      elsif priv == :Column_priv and u.column_perm?
        curp_val = Set.new(curp.Column_priv_before_type_cast.split(/,/).map { |pr| (pr.gsub(" ", '_') + "_priv").capitalize.to_sym })
        u_val    = u.perms_set
      end
      if u_val != curp_val
        changed=true
        break
      end
    end

    # User changed
    if changed
      hs=curp.attributes.merge(:permtype => u.permtype, :server => rd.host, :created_at => u.created_at, :updated_at => rd.runtime, :run_time => rd.runtime)
      if hs.has_key? 'Timestamp'
        hs[:updated_at] = hs['Timestamp']
        hs.delete 'Timestamp'
      end
      if u.table_perm?
        hs.delete 'Table_priv'
        hs.delete 'Column_priv'
      elsif u.column_perm?
        hs.delete 'Column_priv'
      elsif u.proc_perm?
        hs.delete 'Proc_priv'
      end
      newu = rd.stat.new(hs)
      if u.table_perm?
        newu.perms_from_setstr(curp.Table_priv_before_type_cast)
      elsif u.column_perm?
        newu.perms_from_setstr(curp.Column_priv_before_type_cast)
      elsif u.proc_perm?
        newu.perms_from_setstr(curp.Proc_priv_before_type_cast)
      end
      newu.save
      rd.stat_updated(newu.id, u.id)
    end
  end
end

TTT::Formatter.for :user, :text do |stream,frm,data,options|
  col_width=frm.page_width
  unless options[:header]
    stream.puts frm.format('<'*15 + ' ' + '['*(col_width-15), data.server, data.to_s)
  else
    stream.puts frm.format('<'*15 + ' ' + '['*(col_width-15), 'host', 'grant')
  end
end
