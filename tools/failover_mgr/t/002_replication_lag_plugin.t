# 002_replication_lag_plugin.t - crepsucule
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
use TestUtil;
use Test::More tests => 3;
use Test::MockObject::Extends;

BEGIN {
  require_ok($ENV{TOOL});
  fake_use('DSN.pm');
  fake_use('ProcessLog.pm');
  fake_use('FailoverPlugin.pm');
}
use ProcessLog;
BEGIN {
  my $pl = ProcessLog->new($0, 'pdb-test-harness');
  $pl = Test::MockObject::Extends->new($pl);
  no strict 'refs';
  *::PLOG = \$pl;
}
use TestDB;

my $tdb = TestDB->new();

my @opts = (
  '-L', 'pdb-test-harness',
  '-m', 'Dummy',
  '--dummy',
  '--primary', $tdb->dsn(),
  '--failover', $tdb->dsn(),
  '--noplugin', 'ReadOnly',
  '--noplugin', 'AutoIncrement',
  '--noplugin', 'ProcessCounts',
  '--noplugin', 'ReplicationLag'
);
my $rl = ReplicationLag->new();
$rl = Test::MockObject::Extends->new($rl);
eval {
  FailoverManager::main(@opts);
};
is($@, '', 'no croak with no lag');

$rl->mock('get_lag', sub { return 1; });
eval {
  FailoverManager::main(@opts);
};
like($@, qr/Replication lag/, 'croak with lag');
