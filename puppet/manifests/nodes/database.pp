# database.pp
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

node "db0.example.com" {
  include puppet::client

  # by default, mysql::config doesn't overwrite
  # the live my.cnf - this is for migration purposes.
  # setting the below variable will make that happen.
  #
  # $mysql_mycnf_dest = 'sysdefault'
  #
  # OR
  # include mysql::config_overwrite
  #
  # will do the same thing.
  include mysql::config

  # to manage sysctl.conf:
  #
  # include sysctl
  #
  # OR
  #
  # include sysctl_overwrite

  ## Example config

  # if mk-heartbeat is in use, puppet can do that.
  #
  # $mk_heartbeat_ensure = 'running'
  # include maatkit::heartbeat


  # processlist logging can help with post-mortem analysis
  # ensure processlist logging is happening
  # requires storedconfigs
  #
  # proclog { $hostname: }

  # for corp/business intelligence hosts,
  # query sniping can help keep load down, and prevent
  # bad queries from running amok.
  # requires storedconfigs
  #
  # include palomino::querysniper
  # querysniper { $hostname: }

  # our advanced init script:
  # include mysql::mysqlctl
}
