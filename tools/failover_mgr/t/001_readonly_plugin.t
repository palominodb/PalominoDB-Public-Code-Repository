use strict;
use warnings FATAL => 'all';
use Test::More tests => 2;
use ProcessLog;
use TestUtil;
use TestDB;
use FailoverPlugin;
use ReadOnly;

BEGIN {
  my $pl = ProcessLog->new($0, 'pdb-test-harness');
  no strict 'refs';
  *::PLOG = \$pl;
}

my $tdb = TestDB->new();
$tdb->dbh()->do('SET GLOBAL read_only=0');
my $pl = new_ok('ReadOnly');
FailoverPlugin->global_opts('', 0, 1);
$pl->pre_verification($tdb->{dsn}, $tdb->{dsn});
is($pl->{read_only_var}, 0, 'read_only not set');

$pl->post_verification(1, $tdb->{dsn}, $tdb->{dsn});
$pl->{read_only_var} = !$pl->{read_only_var};
$pl->post_verification(1, $tdb->{dsn}, $tdb->{dsn});
