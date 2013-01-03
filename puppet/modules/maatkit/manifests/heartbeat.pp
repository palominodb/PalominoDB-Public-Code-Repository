# heartbeat.pp
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

class maatkit::heartbeat {
  include 'platform'
  schedule { 'kill-heartbeat':
    period => 'hourly',
  }
  $mk_heartbeat_ensure = $mk_heartbeat_ensure ? {
    ''      => 'running',
    default => $mk_heartbeat_ensure,
  }
  $mk_heartbeat_cnf = $mk_heartbeat_cnf ? {
    ''      => "/home/mysql/heartbeat.cnf",
    default => $mk_heartbeat_cnf,
  }
  $mk_heartbeat_db = $mk_heartbeat_db ? {
    ''      => 'heartbeat',
    default => $mk_heartbeat_db,
  }
  $mk_heartbeat_table = $mk_heartbeat_table ? {
    ''      => 'heartbeat',
    default => $mk_heartbeat_table,
  }
  $mk_heartbeat_pidfile = $mk_heartbeat_pidfile ? {
    ''      => '/tmp/mk-heartbeat.pid',
    default => $mk_heartbeat_pidfile,
  }
  
  file { $mk_heartbeat_cnf:
    ensure => 'file',
    owner  => 'root',
    group  => $platform::root_user_group,
    mode   => '0600',
  }
  service { "mk-heartbeat":
    provider => "base",
    start    => "mk-heartbeat --daemonize --pid $mk_heartbeat_pidfile --update -F $mk_heartbeat_cnf --database $mk_heartbeat_db --table $mk_heartbeat_table",
    stop     => "kill `cat /tmp/mk-heartbeat.pid`",
    ensure   => $mk_heartbeat_ensure,
    require  => [File[$mk_heartbeat_cnf],Exec[kill_heartbeat]],
  }

  exec { "/bin/kill `cat /tmp/mk-heartbeat.pid` ; echo 0":
    schedule => 'kill-heartbeat',
    alias    => 'kill_heartbeat',
    before   => Service['mk-heartbeat'],
  }
}
