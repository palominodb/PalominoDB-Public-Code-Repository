# init.pp
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

class puppet {
  # Settings global to puppet go here.

  # This is for sanity. Obviously puppet
  # won't function or be able to install
  # itself if it is missing!
  package { ["puppet", "facter"]:
    ensure => "installed",
    provider => "gem"
  }

  # Other modules may need to know these things.
  $svc_path = $operatingsystem ? {
    "freebsd" => "/etc/rc.d/puppet",
    "centos"  => "/etc/init.d/puppet",
    "redhat"  => "/etc/init.d/puppet",
    "debian"  => "/etc/init.d/puppet",
    "ubuntu"  => "/etc/init.d/puppet",
  }

  $conf_path = $operatingsystem ? {
    "freebsd" => "/etc/puppet/puppet.conf",
    "centos"  => "/etc/puppet/puppet.conf",
    "redhat"  => "/etc/puppet/puppet.conf",
    "debian"  => "/etc/puppet/puppet.conf",
    "ubuntu"  => "/etc/puppet/puppet.conf",
  }
}
