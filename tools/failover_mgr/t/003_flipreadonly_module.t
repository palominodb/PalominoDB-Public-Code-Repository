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
