use strict;
use warnings FATAL => 'all';
use Test::More tests => 4;
use DateTime;
use TestDB;

my $now = DateTime->now( time_zone => 'local' );
BEGIN {
  use_ok('TableAge');
  my $tdb = TestDB->new();
  $tdb->clean_db();
  $tdb->use('table_age');
  $tdb->dbh()->do(qq|
    CREATE TABLE tblz_20100317 (
      `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
      `val` VARCHAR(5)
    ) ENGINE=InnoDB;
    |);
}
my $tdb = TestDB->new();
my $ta = TableAge->new($tdb->dbh(), 'tblz_%Y%m%d');
isa_ok(
  $ta->age_by_status('table_age', 'tblz_20100317'),
  'DateTime',
  'return of age_by_status'
);

is_deeply(
  $ta->age_by_status('table_age', 'tblz_20100317'),
  $now,
  'return of age_by_status is '. $now
);

is_deeply(
  $ta->age_by_name('tblz_20100317'),
  DateTime->new(
    year  => 2010,
    month => 03,
    day   => 17
  ),
  'table name returns DateTime for 2010-03-17'
);
