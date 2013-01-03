# proclog.pp
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

define proclog() {
  include palomino::processlogger
  @@palomino::processlogger::log_host { $name:
    watch_interval => 300,
    thread_limit   => 1,
    host_cnf       => "${palomino::cfg_dir}/ops.cnf",
    log_dsn        => "h=localhost,F=${palomino::cfg_dir}/ops.cnf,D=palomino,t=process_list,i=3",
    ensure         => 'running',
    tag            => 'on_ops',
  }
}
