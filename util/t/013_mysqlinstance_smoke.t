# 013_mysqlinstance_smoke.t
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
use Test::More tests => 6;
use TestUtil;
BEGIN {
  use_ok('MysqlInstance');
}

my $ssh_host = $ENV{TEST_SSH_HOST};
my $ssh_user = $ENV{TEST_SSH_USER};

my $l = new_ok('MysqlInstance' => ['localhost']);

my $res = MysqlInstance::_action(undef, 'hostname');
is($res->[0], 'debian-lenny.i.linuxfood.net', '_action has right hostname');

is_deeply($l->_do('hostname'), 'debian-lenny.i.linuxfood.net', '_do has right hostname');

SKIP: {
  skip 'Need TEST_SSH_HOST and TEST_SSH_USER setup', 2 if(!$ssh_host or !$ssh_user);
  my $r = new_ok('MysqlInstance' => [$ssh_host, $ssh_user]);
  is($r->_do('hostname'), $ssh_host, 'remote hostname matches');
}
