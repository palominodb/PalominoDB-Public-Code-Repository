use strict;
use warnings FATAL => 'all';
use Test::More tests => 2;

BEGIN {
  require_ok($ENV{TOOL});
}

my @opts = (
  '-L', 'pdb-test-harness',
  '-m', 'Dummy',
  '--dummy',
  '--primary', 'h=localhost',
  '--failover', 'h=localhost',
  '--noplugin', 'AutoIncrement',
  '--noplugin', 'ReplicationLag',
  '--noplugin', 'ReadOnly',
  '--noplugin', 'ProcessCounts'
);

is(FailoverManager::main(@opts), 0, 'can run main()');
