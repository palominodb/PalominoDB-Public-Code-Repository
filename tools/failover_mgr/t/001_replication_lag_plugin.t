use strict;
use warnings FATAL => 'all';
use TestUtil;
use TestDB;
use Test::More tests => 4;
use ProcessLog;
use Test::MockObject::Extends;
use FailoverPlugin;
use ReplicationLag;

BEGIN {
  my $pl = ProcessLog->new($0, 'pdb-test-harness');
  $pl = Test::MockObject::Extends->new($pl);
  no strict 'refs';
  *::PLOG = \$pl;
}

my $tdb = TestDB->new();
my $fp = ReplicationLag->new();
$fp = Test::MockObject::Extends->new($fp);

$fp->mock('get_lag', sub { return 0; });
eval {
  $fp->pre_verification($tdb->{dsn});
};

is($@, '', 'no croak with no lag');

$fp->mock('get_lag', sub { return 5; });
eval {
  $fp->pre_verification($tdb->{dsn});
};
like($@, qr/Replication lag/, 'croak with lag');

FailoverPlugin->global_opts('', 0, 1); # enable force
$::PLOG->mock('p', sub { return 'no' });
eval {
  $fp->pre_verification($tdb->{dsn});
};
like($@, qr/Replication lag/, 'croak with lag, force, and "no" response');
$::PLOG->mock('p', sub { return 'yes' });
eval {
  $fp->pre_verification($tdb->{dsn});
};
is($@, '', 'no croak with lag, force, and "yes" response');
