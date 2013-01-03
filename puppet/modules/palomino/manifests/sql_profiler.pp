# sql_profiler.pp
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

class palomino::sql_profiler {
  include palomino
  file { "${palomino::bin_dir}/sql_profiler.sh":
    owner => 'root',
    group => 'root',
    mode  => '0755',
    source => 'puppet:///modules/palomino/sql_profiler.sh',
  }

  define profile($log_path, $ttt_name, $cfg_file, $email_to ) {
    file { "$cfg_file":
      ensure => "file",
      mode   => "0644",
      owner  => "root",
      group  => "root",
      content => template("palomino/sql_profiler_cfg.erb"),
    }
  }
}
