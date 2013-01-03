# user.pp
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

class mysql::user {
  $homedir = $mysql_homedir ? {
    '' => "/home/mysql",
    default => $mysql_homedir,
  }
  $shell = $mysql_usershell ? {
    '' => $operatingsystem ? {
      "freebsd" => "/usr/local/bin/bash",
      "centos"  => "/bin/bash",
      "redhat"  => "/bin/bash",
      "ubuntu"  => "/bin/bash",
      "debian"  => "/bin/bash",
      },
    default => $mysql_usershell
  }
  user { "mysql":
    uid => '88',
    gid => '88',
    home => $homedir,
    ensure => 'present',
    shell  => $shell,
  }

  file { "$homedir":
    ensure => 'directory',
    owner  => 'mysql',
    group  => 'mysql',
    mode   => '0755',
  }

  file { "$homedir/.ssh":
    ensure => 'directory',
    owner => 'mysql',
    group => 'mysql',
    mode  => '0700',
  }

  ssh_authorized_key{ 'slow_query_profiler@ops':
    key => 'AAAAB3NzaC1yc2EAAAABIwAAAIEAvFdoxqjLJBAHkCstFGTvKeEonGUOS80XqSTzpflk24GGISDIHhTkU+JUFFcqU8Plji1fgufpVZYltiOE/C0zFWI1GJNi4xbVjwc3Ez2YWG+8aAKxyJ4KfFbjpZIb95zi7NHSu5hAqBJtH6HQWUZtrE3OXbDyexTqPp0gaOncvuk=',
    type => 'ssh-rsa',
    user => 'mysql',
  }
}
