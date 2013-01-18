# 002_autoincrement_plugin.t - crepsucule
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
use Test::More tests => 3;
use TestUtil;
BEGIN {
  require_ok($ENV{TOOL});
  fake_use('DSN.pm');
}
use TestDB;

my $tdb = TestDB->new();

my @opts = (
  '-L', 'pdb-test-harness',
  '-m', 'Dummy',
  '--dummy',
  '--primary', $tdb->dsn(),
  '--failover', $tdb->dsn(),
  '--noplugin', 'ReplicationLag',
  '--noplugin', 'ReadOnly',
  '--noplugin', 'ProcessCounts'
);

eval {
  FailoverManager::main(@opts);
};
like($@, qr/Failed pre-verification check: auto_increment_offset/, 'dies without --force from main()');
unshift @opts, '--force';
eval {
  FailoverManager::main(@opts);
};
is($@, '', 'does not die with --force from main()');
