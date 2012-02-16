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
