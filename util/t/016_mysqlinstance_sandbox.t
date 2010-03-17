use strict;
use warnings FATAL => 'all';
use Test::More tests => 7;
use TestUtil;
use MysqlInstance;

my $msb_path = $ENV{TEST_SANDBOX_PATH};
SKIP: {
  skip 'need TEST_SANDBOX_PATH setup', 7 unless($msb_path);
  diag('These tests can take a very long time. Be patient.');
  # This primes the sandbox
  # Since this tests needs to test 'start'
  system("$msb_path/stop");
  my $meths = MysqlInstance::Methods->new(
    "$msb_path/start 2>&1 | head -n1 | grep -vqE 'not|already'",
    "$msb_path/stop 2>&1 | wc -c | grep -q 0",
    "$msb_path/restart 2>&1 | head -n1 | grep -vqE 'not|already'",
    "$msb_path/status &>/dev/null",
    "$msb_path/my.sandbox.cnf"
  );
  my $l = MysqlInstance->new('localhost');
  $l->methods($meths);
  is($l->start, 0, 'start returns successfully');
  is($l->stop, 0, 'stop returns successfully');
  is($l->status, 1, 'status for stopped returns successfully');
  is($l->restart, 0, 'restart returns successfully');
  is($l->status, 0, 'status for started returns successfully');

  my $dbh = $l->get_dbh('msandbox', 'msandbox');
  is(ref($dbh), 'DBI::db', 'successfully get dbh');
  is($dbh->selectcol_arrayref('SELECT 1;')->[0], 1, 'can execute query');
  $dbh->disconnect;
}
