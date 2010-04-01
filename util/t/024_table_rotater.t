use strict;
use warnings FATAL => 'all';
use Test::More tests => 10;
use TestDB;
use DateTime;

BEGIN {
  use_ok('TableRotater');
  my $tdb = TestDB->new();
  $tdb->clean_db();
  $tdb->use('table_rotate');
  $tdb->dbh()->do(qq|
    CREATE TABLE abcde (
      `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
      `vl` VARCHAR(10)
    ) Engine=InnoDB;
    |);
}

my $tdb = TestDB->new();
srand(10); # Make random names always the same
my $date = DateTime->new( year => 2010, month => 03, day => 17 );
my $tr  = TableRotater->new($tdb->{dsn}, "_%Y%m%d");

is($tr->date_rotate_name('abcde', $date), 'abcde_20100317', 'rotate name');
is($tr->date_rotate('table_rotate', 'abcde', $date), 'abcde_20100317', 'rotate returns name');
is($tr->rand_str(), 'p9NPhhjnnWgDCONw', 'random string is fixed in test');
diag('OK if the previous test fails.');
eval {
  $tr->date_rotate('table_rotate', 'abcdf', $date);
};
like($@, qr/^Unable to create new table/, 'cannot rotate nonexistant table');

eval {
  $tr->date_rotate('table_rotate', 'abcde', $date);
};
like($@, qr/^Failed to rename table to abcde_20100317/, 'cannot rotate table to same date');

diag('Testing alternate/weird date formats');
$tr = TableRotater->new($tdb->{dsn}, "_%Y_%V");
is($tr->date_rotate('table_rotate', 'abcde', $date), 'abcde_2010_11', 'Year-Week format');
$tr = TableRotater->new($tdb->{dsn}, "_%j");
is($tr->date_rotate('table_rotate', 'abcde', $date), 'abcde_76', 'Day of year format');
$tr = TableRotater->new($tdb->{dsn}, "_%R");
is($tr->date_rotate('table_rotate', 'abcde', $date), 'abcde_00:00', 'HH:MM format');
$tr = TableRotater->new($tdb->{dsn}, "_%a, %d %b %Y %H:%M:%S %z");
is($tr->date_rotate('table_rotate', 'abcde', $date), 'abcde_Wed, 17 Mar 2010 00:00:00 +0000', 'RFC822 date');
