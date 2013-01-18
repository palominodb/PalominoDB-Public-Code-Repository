# processlogger.pp - a Puppet class for installing the processlogger module used in other PDB tools.
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

class palomino::processlogger {
  include palomino
  # perl /usr/local/pdb/bin/mk-loadavg -F /usr/local/pdb/bin/ops.cnf --watch Status:status:Threads_connected:>:1000 --plugin ProcesslistLogger;h=localhost,u=log,D=palomino,t=process_list,p=noop -h 10.10.1.142 --interval 30 --daemonize
  define log_host($watch_interval, $host_cnf, $log_dsn, $thread_limit, $ensure = 'running') {
    service { "processlogger_${name}":
      provider => "base",
      start    => "perl -I ${palomino::bin_dir} ${palomino::mk_loadavg} -F $host_cnf --watch 'Status:status:Threads_connected:>:$thread_limit' --plugin 'ProcesslistLogger;$log_dsn' --host $name --interval $watch_interval --daemonize --pid /tmp/processlist_logger_${name}.pid",
      stop     => "kill `cat /tmp/processlist_logger_${name}.pid`",
      status   => "ps auxxx | grep -E '.*ProcesslistLogger.*${name}' | grep -v grep",
      ensure   => $ensure,
    }
  }
}
