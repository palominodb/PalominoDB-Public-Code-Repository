# 018_robj_add_package.t
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
use RObj;
use MysqlMasterInfo;
use Test::More tests => 1;

my $ssh_host = $ENV{TEST_SSH_HOST};
my $ssh_user = $ENV{TEST_SSH_USER};


SKIP: {
  skip 'Need test SSH setup.', 1 if(!$ssh_host or !$ssh_user);
  my $rmi = RObj->new($ssh_host, $ssh_user);
  $rmi->add_package('MysqlMasterInfo');
  $rmi->add_main(sub { MysqlMasterInfo->open('/fake/master.info'); });
  is_deeply([$rmi->do()], ['EXIT', MysqlMasterInfo->open('/fake/master.info')], 'remote master info');
}
