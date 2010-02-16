require 'rubygems'
require 'active_record'
require 'ttt/table'

module TTT
  class TableUser < ActiveRecord::Base
    include TrackingTable
    self.collector = :user

    # Yeah, I know that bitmaps in a database are sorta a 'no no'
    # But, I think in this instance it's worth the trouble.

    # Mapping to the permtype column
    # This is less involved than 'computing' the permission type.
    GLOBAL_PERMISSION = 1<<4
    HOST_PERMISSION   = 1<<5
    DB_PERMISSION     = 1<<6
    TABLE_PERMISSION  = 1<<7
    COLUMN_PERMISSION = 1<<8
    PROC_PERMISSION   = 1<<9

    # This is or-ed with the above to mark a permission as deleted.
    DELETED_PERMISSION = 1<<1
    # These two bits are reserved for later use.
    UNREACHABLE_ENTRY  = 1<<2
    RESERVED_PERMISSION2 = 1<<3

    PRIV_FLAG_COLUMNS = [
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
      :File_priv,
      :Create_user_priv,
      :Process_priv,
      :Reload_priv,
      :Repl_client_priv,
      :Repl_slave_priv,
      :Show_db_priv,
      :Shutdown_priv,
      :Super_priv,
    ]

    ALL_GLOBAL_PRIVS = (PRIV_FLAG_COLUMNS - [:Grant_priv])
    ALL_DB_PRIVS   = [:Select_priv, :Insert_priv, :Update_priv,
      :Delete_priv, :Create_priv, :Drop_priv, :References_priv,
      :Index_priv, :Alter_priv, :Create_tmp_table_priv,
      :Lock_tables_priv, :Create_view_priv, :Show_view_priv,
      :Create_routine_priv, :Execute_priv, :Event_priv, :Trigger_priv]
    ALL_HOST_PRIVS =  ALL_DB_PRIVS
    ALL_TABLE_PRIVS  = [:Select_priv, :Insert_priv, :Update_priv,
      :Delete_priv, :Create_priv, :Drop_priv, :References_priv,
      :Index_priv, :Alter_priv, :Create_view_priv, :Show_view_priv,
      :Trigger_priv]
    ALL_COLUMN_PRIVS = [:Select_priv, :Insert_priv, :Update_priv, :References_priv]
    ALL_PROC_PRIVS = [:Execute_priv, :Alter_routine_priv]

    def database_name
      schema
    end

    def table_name
      self.Table_name
    end

    def schema
      self.Db
    end
    def schema=(s)
      self.Db=s
    end

    # Returns permissions as a set
    def perms_set
      PRIV_FLAG_COLUMNS.select { |pfc| self[pfc] == 'Y' }.to_set
    end

    # Sets permissions from a set
    def perms_from_set(s)
      throw ArgumentError, "Invalid columns" unless s.subset? PRIV_FLAG_COLUMNS.to_set
      s.each { |i| self[i] = 'Y' }
      true
    end

    # Sets permissions from a comma separated set string
    def perms_from_setstr(s)
      perms_from_set(Set.new(s.split(/,/).map { |p| (p.gsub(' ', '_') + "_priv").capitalize.to_sym }))
    end

    def self.perms_from_setstr(s)
      sn=Set.new(s.split(/,/).map { |p| (p.gsub(' ', '_') + "_priv").capitalize.to_sym })
      throw ArgumentError, "Invalid columns" unless sn.subset? PRIV_FLAG_COLUMNS.to_set
      sn
    end

    def unreachable?
      (permtype & UNREACHABLE_ENTRY) != 0
    end

    def deleted?
      (permtype & DELETED_PERMISSION) != 0
    end

    def global_perm?
      (permtype & GLOBAL_PERMISSION) != 0
    end

    def host_perm?
      (permtype & HOST_PERMISSION) != 0
    end

    def db_perm?
      (permtype & DB_PERMISSION) != 0
    end

    def table_perm?
      (permtype & TABLE_PERMISSION) != 0
    end

    def column_perm?
      (permtype & COLUMN_PERMISSION) != 0
    end

    def proc_perm?
      (permtype & PROC_PERMISSION) != 0
    end

    def perm_to_s(perm)
      perm.to_s.upcase.gsub('_', ' ').gsub(' PRIV', '').gsub('REPL', 'REPLICATION').gsub('TMP TABLE', 'TEMPORARY TABLES')
    end

    def to_s
      gstr='GRANT '
      perms=(perms_set.delete :Grant_priv)
      has_grant=(perms_set.member? :Grant_priv)
      pw=((Password().nil? or Password().empty?) ? '' : "IDENTIFIED BY PASSWORD '#{Password()}'")
      on_ref = 'ON *.*'
      user_ref = "'#{User()}'@'#{Host().nil? or Host().empty? ? '%' : Host()}'"
      priv_massage = Proc.new { |pstr| pstr }
      privstype_set=case permtype & ~0x3
      when GLOBAL_PERMISSION
        ALL_GLOBAL_PRIVS.to_set
      when HOST_PERMISSION
        if deleted?
          return "DELETE FROM  `mysql`.`host` WHERE Host='#{Host()}' AND Db='#{Db()}'"
        else
          return "REPLACE INTO `mysql`.`host` (Host,Db,#{perms.to_a.sort.join(',')}) VALUES ('#{Host()}','#{Db()}',#{perms_to_a.sort.map{|c| "'Y'"}.join(',')})"
        end
      when DB_PERMISSION
        on_ref = "ON `#{Db()}`.*"
        ALL_DB_PRIVS.to_set
      when TABLE_PERMISSION
        on_ref = "ON `#{Db()}`.`#{Table_name()}`"
        ALL_TABLE_PRIVS.to_set
      when COLUMN_PERMISSION
        on_ref = "ON `#{Db()}`.`#{Table_name()}`"
        priv_massage = Proc.new { |p| "#{p} (#{Column_name()})" }
        ALL_COLUMN_PRIVS.to_set
      when PROC_PERMISSION
        ALL_PROC_PRIVS.to_set
      else
        self.inspect
      end

      if perms.superset? privstype_set
        gstr += priv_massage.call('ALL PRIVILEGES') + ' '
      elsif perms.empty?
        gstr += 'USAGE '
      else
        gstr += perms.map { |p| priv_massage.call(perm_to_s(p)) }.join(', ') + ' '
      end
      gstr += [on_ref, 'TO', user_ref, pw, (has_grant ? 'WITH GRANT OPTION' : '')].join(' ')
      if deleted?
        "DROP USER #{user_ref}"
      else
        gstr
      end
    end

    def tchanged?
      last=previous_version()
      last.nil? or last.deleted? or perms_set != last.perms_set
    end

    def previous_version
      extra_cons,extra_cons_i=case permtype & ~0x3
                              when GLOBAL_PERMISSION
                                previous_global
                              when HOST_PERMISSION
                                previous_host
                              when DB_PERMISSION
                                previous_db
                              when TABLE_PERMISSION
                                previous_table
                              when COLUMN_PERMISSION
                                previous_column
                              when PROC_PERMISSION
                                previous_proc
                              end
      self.class.last(:conditions => ['id < ? AND permtype & ? != 0 AND server = ?' + ' AND ' + extra_cons, id, permtype & ~0x3 , server] + extra_cons_i)
    end

    def self.create_unreachable_entry(host, runtime)
      TTT::TableUser.new(
        :server => host,
        :run_time => runtime,
        :permtype => UNREACHABLE_ENTRY,
        :created_at => '0000-00-00 00:00:00',
        :updated_at => '0000-00-00 00:00:00'
      )
    end

    protected
    def previous_global
      ['Host = ? AND User = ?', [Host(), User()]]
    end

    def previous_host
      ['Host = ? AND Db = ?', [Host(), Db()]]
    end

    def previous_db
      ['Host = ? AND Db = ? AND User = ?', [Host(), Db(), User()]]
    end

    def previous_table
      ['Host = ? AND Db = ? AND User = ? AND Table_name = ?', [Host(), Db(), User(), Table_name()]]
    end

    def previous_column
      ['Host = ? AND Db = ? AND User = ? AND Table_name = ? AND Column_name = ?', [Host(), Db(), User(), Table_name(), Column_name()]]
    end

    def previous_proc
      ['Host = ? AND Db = ? AND User = ? AND Routine_name = ? AND Routine_type = ?', [Host(), Db(), User(), Routine_name(), Routine_type()]]
    end
  end # TTT::TableUser
end # TTT
