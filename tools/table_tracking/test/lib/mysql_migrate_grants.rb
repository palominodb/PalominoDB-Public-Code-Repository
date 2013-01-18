# mysql_migrate_grants.rb
# Copyright (C) 2013 PalominoDB, Inc.
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

module ActiveRecord
  module ConnectionAdapters
    module SchemaStatements

      def grant_global(user, host, privs, password=nil)
        grant_global_sql=
          "GRANT #{privs.join(', ')} ON *.* TO '#{user}'@'#{host}'"
        grant_global_sql += " IDENTIFIED BY '#{password}'" if password
        execute(grant_global_sql)
      end

      def revoke_global(user,host, privs)
        revoke_global_sql=
          "REVOKE #{privs.join(', ')} ON *.* FROM '#{user}'@'#{host}'"
        execute(revoke_global_sql)
      end

      def grant_db(user, host, db, privs, password=nil)
        grant_sql=
          "GRANT #{privs.join(', ')} ON `#{db}`.* TO '#{user}'@'#{host}'"
        grant_sql += " IDENTIFIED BY '#{password}'" if password
        execute(grant_sql)
      end

      def revoke_db(user, host, db, privs)
        revoke_sql =
          "REVOKE #{privs.join(', ')} ON `#{db}`.* FROM '#{user}'@'#{host}'"
        execute(revoke_sql)
      end

      def grant_tbl(user, host, db, tbl, privs, password=nil)
        grant_sql=
          "GRANT #{privs.join(', ')} ON `#{db}`.`#{tbl}` TO '#{user}'@'#{host}'"
        grant_sql += " IDENTIFIED BY '#{password}'" if password
        execute(grant_sql)
      end

      def revoke_tbl(user, host, db, tbl, privs)
        revoke_sql =
          "REVOKE #{privs.join(', ')} ON `#{db}`.`#{tbl}` FROM '#{user}'@'#{host}'"
        execute(revoke_sql)
      end

      def grant_col(user, host, db, tbl, privs, password=nil)
        grant_sql= "GRANT "
        privs.each do |p|
          grant_sql += p.shift
          if p.length > 0
            grant_sql += "(#{p.join(',')}),"
          end
        end
        grant_sql.chop!

        grant_sql += " ON `#{db}`.`#{tbl}` TO '#{user}'@'#{host}'"
        grant_sql += " IDENTIFIED BY '#{password}'" if password
        execute(grant_sql)
      end

      def revoke_col(user, host, db, tbl, privs, password=nil)
        revoke_sql= "REVOKE "
        privs.each do |p|
          revoke_sql += p.shift
          if p.length > 0
            revoke_sql += "(#{p.join(',')}),"
          end
        end
        revoke_sql.chop!

        revoke_sql += " ON `#{db}`.`#{tbl}` FROM '#{user}'@'#{host}'"
        execute(revoke_sql)
      end

      def grant_proc(user, host, db, routine_name, privs, password=nil)
        grant_sql = "GRANT #{privs.join(', ')} ON PROCEDURE `#{db}`.`#{routine_name}` TO '#{user}'@'#{host}'"
        execute(grant_sql)
      end

      def revoke_proc(user, host, db, routine_name, privs)
        revoke_sql = "REVOKE #{privs.join(', ')} ON PROCEDURE `#{db}`.`#{routine_name}` FROM '#{user}'@'#{host}'"
        execute(revoke_sql)
      end

      def drop_user(user,host)
        execute("DROP USER '#{user}'@'#{host}'")
      end

    end
  end
end
