# 003_moveslaves_module.t
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
no warnings 'once';
use Test::More tests => 2;
use TestUtil;

BEGIN {
  require_ok($ENV{TOOL});
  fake_use('DSN.pm');
}

use TestDB;
my $tdb = TestDB->new();

my @opts = (
  '-L', 'pdb-test-harness',
  '-m', 'MoveSlaves',
  '--primary', $tdb->dsn(),
  '--failover', $tdb->dsn(),
  '--slave', $tdb->dsn(),
  '--noplugin', 'AutoIncrement',
  '--noplugin', 'ReadOnly',
  '--noplugin', 'ProcessCounts',
  '--noplugin', 'ReplicationLag'
);


TODO: {
  local $TODO = 'pending test framework updates';
  eval {
    FailoverManager::main(@opts);
  };

  is($@, '', 'no croak');
}
