# init.pp - a puppet class for installing mk-loadavg; used as a basis for other
# tool installations.
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

class palomino {
  # Where the tools are staged
  $install_dir = '/usr/local/pdb'
  $bin_dir = "$install_dir/bin"
  $cfg_dir = "$install_dir/etc"

  # PalominoDB patched mk-loadavg
  $mk_loadavg = "$bin_dir/mk-loadavg"

  file { $install_dir:
    ensure => 'directory',
    owner => 'root',
    group => 'root',
    mode  => '0755',
  }

  file { $bin_dir:
    ensure => 'directory',
    owner => 'root',
    group => 'root',
    mode  => '0755',
  }

  file { $cfg_dir:
    ensure => 'directory',
    owner => 'root',
    group => 'root',
    mode  => '0755',
  }

  file { $mk_loadavg:
    ensure => 'file',
    owner => 'root',
    group => 'root',
    mode  => '0755',
    source => 'puppet:///modules/palomino/mk-loadavg',
  }

}
