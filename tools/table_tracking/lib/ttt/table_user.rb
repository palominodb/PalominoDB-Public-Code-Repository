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
    RESERVED_PERMISSION1 = 1<<2
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
  end
end
