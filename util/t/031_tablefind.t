use strict;
use warnings FATAL => 'all';
use Test::More tests => 6;
use TestDB;
BEGIN {
  $TestDB::dsnstr .= ",D=find_table";
  use_ok('TableFind');
  my $tdb = TestDB->new();
  $tdb->clean_db();
  $tdb->use('find_table');
  $tdb->dbh()->do(qq|
    CREATE TABLE table_20(x int);
  |);
  $tdb->dbh()->do(qq|
    CREATE TABLE table_19(x int);
  |);
  $tdb->dbh()->do(qq|
    CREATE TABLE bobtable_20(x int);
  |);
}

my $tdb = TestDB->new();
my $tf = new_ok('TableFind' => [$$tdb{dsn}]);

eval {
  $tf->find(notreal => 1);
};
like($@, qr/Unknown predicate.*/, "croak on unknown predicate");

ok($tf->find(name => qr/^bob/), "find table by name");
ok(!$tf->find(name => qr/^stan/), "no match table by name");
ok($tf->find(engine => 'myisam'), "find table by engine");
