# 003_flipreadonly_module.t
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
use Test::More tests => 5;
use TestUtil;

BEGIN {
  require_ok($ENV{TOOL});
  fake_use('DSN.pm');
}
use TestDB;

my $tdb = TestDB->new();
my @opts = (
  '-L', 'pdb-test-harness',
  '-m', 'FlipReadOnly',
  '--primary', $tdb->dsn(),
  '--failover', $tdb->dsn(),
  '--noplugin', 'AutoIncrement',
  '--noplugin', 'ReadOnly',
  '--noplugin', 'ProcessCounts',
  '--noplugin', 'ReplicationLag'
);

$tdb->dbh()->do('SET GLOBAL read_only=1');

eval {
  FailoverManager::main(@opts);
};
is($@, '', 'no croak');
is($tdb->dbh()->selectcol_arrayref('SELECT @@read_only')->[0], 0, 'read_only changed');

$tdb->dbh()->do('SET GLOBAL read_only=1');
eval {
  unshift @opts, '--pretend';
  FailoverManager::main(@opts);
};
is($@, '', 'no croak');
is($tdb->dbh()->selectcol_arrayref('SELECT @@read_only')->[0], 1, 'read_only not changed');
