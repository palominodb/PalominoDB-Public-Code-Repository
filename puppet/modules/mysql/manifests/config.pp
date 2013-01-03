# config.pp
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

class mysql::config {
  require mysql
  # Default to placing in /tmp for safety.
  # Location can be set explicily inside including classes.
  $dest = $mysql_mycnf_dest  ? {
    '' => "/tmp/my.cnf.puppet_managed",
    "sysdefault" => $mysql::my_cnf_path,
    default => $mysql_mycnf_dest,
  }
  file { 'my.cnf':
    path   => $dest,
    ensure => "file",
    owner => "root",
    group => $operatingsystem ? {
      freebsd => "wheel",
      default => "root",
    },
    source => "puppet:///git-config/mysql/$ipaddress/my.cnf",
  }
}

