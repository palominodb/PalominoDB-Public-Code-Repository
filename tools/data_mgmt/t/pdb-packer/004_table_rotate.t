use strict;
use warnings FATAL => 'all';
use Test::More tests => 2;
use TestDB;

BEGIN {
  require_ok('src/pdb-packer');
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
srand(10);
$pdb_packer::cur_date = DateTime->new( year => 2010, month => 04, day => 17 );
my $tr = TableRotater->new($tdb->{dsn}, "_%Y%m%d");
$tdb->{dsn}->{'t'}->{'value'} = 'abcde';
$tdb->{dsn}->{'D'}->{'value'} = 'table_rotate';
eval {
  is(pdb_packer::rotate_table($tr, $tdb->{dsn})->{'t'}->{'value'}, 'abcde_20100417', 'returns rotated name');
};
if($@) {
  diag($tr->{errstr});
  fail('returns rotated name');
}
