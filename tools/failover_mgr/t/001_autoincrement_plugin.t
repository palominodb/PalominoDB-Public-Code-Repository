use strict;
use warnings FATAL => 'all';
use Test::More tests => 3;
use ProcessLog;
use TestUtil;
use TestDB;
use FailoverPlugin;
use AutoIncrement;
BEGIN {
  my $pl = ProcessLog->new($0, 'pdb-test-harness');
  no strict 'refs';
  *::PLOG = \$pl;
}

my $tdb = TestDB->new();
my $pl = new_ok('AutoIncrement');
eval {
  $pl->pre_verification($tdb->{dsn}, $tdb->{dsn});
};
like($@, qr/Failed pre-verification check/, 'dies when offsets identical');

eval {
  no warnings 'once';
  $FailoverPlugin::force = 1;
  $pl->pre_verification($tdb->{dsn}, $tdb->{dsn});
};
is($@, '', 'does not die when --force');
