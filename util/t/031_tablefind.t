use strict;
use warnings FATAL => 'all';
use Test::More tests => 12;
use TestDB;
use DateTime;

BEGIN {
  use_ok('TableFind');
  my $tdb = TestDB->new();
  $tdb->clean_db();
  $tdb->use('find_table');
  $TestDB::dsnstr .= ",D=find_table";
  $tdb->dbh()->do(qq|
    CREATE TABLE table_20(x int) ENGINE=InnoDB;
  |);
  $tdb->dbh()->do(qq|
    CREATE TABLE table_19(x int) ENGINE=MyISAM;
  |);
  $tdb->dbh()->do(qq|
    CREATE TABLE bobtable_20(x int);
  |);

  $tdb->dbh()->do(qq|
    CREATE TABLE test_20101101 (x int);
  |);
  $tdb->dbh()->do(qq|
    CREATE TABLE test_20101104 (x int);
  |);
  $tdb->dbh()->do(qq|
    CREATE TABLE test_20101108 (x int);
  |);
  $tdb->dbh()->do(qq|
    CREATE TABLE test_20101116 (x int);
  |);
  $tdb->dbh()->do(qq|
    CREATE TABLE test_20101202 (x int);
  |);
}

my $tdb = TestDB->new();
my $tf = new_ok('TableFind' => [$$tdb{dsn}]);

eval {
  $tf->find('bad_predicate_list');
};
like($@, qr/Uneven number of predicates and arguments.*/,
     "croak on uneven predicate list");

eval {
  $tf->find(notreal => 1);
};
like($@, qr/Unknown predicate.*/, "croak on unknown predicate");

ok($tf->find(name => qr/^bob/), "find table by name");
ok(!$tf->find(name => qr/^stan/), "no match table by name");
ok($tf->find(engine => 'myisam'), "find table by engine");

is_deeply([map { $_->{Name} } $tf->find(engine => 'innodb', name => qr/^table/)],
          ['table_20'], "predicates AND, and short-circuit");

my $pattern = "test_%Y%m%d";
my $date = DateTime->new(
      year  => 2010,
      month => 11,
      day   => 4);
ok($tf->find(
  name => qr/^test_/,
  agebyname => { older_than => DateTime->now(),
                 pattern => $pattern },
),
   "find table older_than");

ok(!$tf->find(agebyname => { newer_than => DateTime->now(),
                             pattern => $pattern }),
   "find table newer_than");

is_deeply([map { $_->{Name} } $tf->find(
  agebyname => {
    older_than => $date,
    eq_to      => $date,
    pattern => $pattern
  })], [ "test_20101101", "test_20101104" ],
          "find older_than, verify list");

is_deeply([map { $_->{Name} } $tf->find(
  agebyname => {
    newer_than => $date,
    eq_to      => $date,
    pattern => $pattern
  })], [ "test_20101104", "test_20101108", "test_20101116", "test_20101202" ],
          "find newer_than, verify list");
