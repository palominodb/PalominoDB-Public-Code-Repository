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
