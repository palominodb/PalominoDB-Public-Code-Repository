# querysniper.pp - a Puppet class for installing the PalominoDB query sniper tool.
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

class palomino::querysniper {
  include palomino
  file { "${palomino::bin_dir}/QuerySniper.pm":
    owner => 'root',
    group => 'root',
    mode  => '0644',
    source => 'puppet:///modules/palomino/QuerySniper.pm',
  }
  define sniper_host($watch_interval, $host_cnf, $sniper_rules, $ensure = 'running') {
    service { "querysniper_${name}":
      provider => "base",
      start    => "perl -I ${palomino::bin_dir} ${palomino::mk_loadavg} -F $host_cnf --watch 'Processlist:command:Query:time:>:-1' --plugin 'QuerySniper;$sniper_rules' --host $name --interval $watch_interval --daemonize --pid /tmp/querysniper_${name}.pid",
      stop     => "kill `cat /tmp/querysniper_${name}.pid`",
      status   => "ps auxxx | grep -E '.*QuerySniper.*${name}' | grep -v grep",
      ensure   => $ensure,
      subscribe => File["$sniper_rules", "${palomino::bin_dir}/QuerySniper.pm"],
    }
  }
}
