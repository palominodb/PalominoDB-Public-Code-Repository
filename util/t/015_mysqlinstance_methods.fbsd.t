# 015_mysqlinstance_methods.fbsd.t
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

use strict;
use warnings FATAL => 'all';
use Test::More tests => 5;
use TestUtil;
use MysqlInstance;

SKIP: {
  skip 'this only valid on FreeBSD', 5 if($^O ne 'freebsd');
  my $mths = MysqlInstance::Methods->detect;
  is($mths->{start}, '/etc/init.d/mysql start &>/dev/null', 'start cmd');
  is($mths->{stop}, '/etc/init.d/mysql stop &>/dev/null', 'stop cmd');
  is($mths->{restart}, '/etc/init.d/mysql restart &>/dev/null', 'restart cmd');
  is($mths->{status}, '/etc/init.d/mysql status &>/dev/null', 'restart cmd');
  is($mths->{config}, '/etc/mysql/my.cnf', 'config path');
  my $server_pkg;
  chomp($server_pkg = qx{pkg_info | grep mysql-server});
  diag("It's OK to ignore these failures since you don't appear to have mysql-server installed") unless($server_pkg);
}
