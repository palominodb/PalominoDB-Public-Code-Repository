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
