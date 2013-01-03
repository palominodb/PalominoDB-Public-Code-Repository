# mysqlctl_package.pp
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

class mysqlctl::package {
  $pkg_version = '0.04-2'
  $pkg_vendor  = 'site'
  package {
    'mysqlctl':
      ensure => $pkg_version;
    "mysqlctl-init-$pkg_vendor":
      ensure => $pkg_version,
      require => Package[mysqlctl];
  }
    
  service { 'mysql':
    enable  => 'false',
    require => Package["mysqlctl-init-$pkg_vendor"],
  }

  service { "$pkg_vendor-mysql":
    enable     => 'true',
    hasstatus  => 'true',
    hasrestart => 'false',
    require    => [Service[mysql], Package["mysqlctl-init-$pkg_vendor"]],
  }

  # Optional:
  #file { '/etc/init.d/mysql':
  #  ensure => 'file',
  #  owner  => 'root',
  #  group  => 'root',
  #  mode   => '0755',
  #  content => "#!/bin/sh\necho Use /etc/init.d/$pkg_vendor-mysql instead.\nexit 1",
  #}
}
