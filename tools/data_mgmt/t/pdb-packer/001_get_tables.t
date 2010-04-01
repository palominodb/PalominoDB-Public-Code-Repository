use strict;
use warnings FATAL => 'all';
use Test::More tests => 4;
use TestDB;
use DSN;

BEGIN {
  require_ok('src/pdb-packer.in.pl');
  my $tdb = TestDB->new();
  $tdb->clean_db();
  $tdb->use('pdb_packer');
  for((1...10)) {
    my $eng = ($_ % 2 ? 'myisam' : 'innodb');
    $tdb->dbh()->do(qq|
      CREATE TABLE prfx_$_ (
        `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
        `name` VARCHAR(60) NOT NULL
      ) Engine=$eng;
      |);
  }
}

END {
  #TestDB->new()->clean_db();
}
my $tdb  = TestDB->new();
my $dsnp = DSNParser->default();
$dsnp->add_key('r', { 'mandatory' => 0, 'desc' => '' });
my $dsn1 = $dsnp->parse($tdb->dsn() . ",D=pdb_packer,t=prfx_5");
my $dsn2 = $dsnp->parse($tdb->dsn() . ",D=pdb_packer,r=prfx_1");
my $dsn3 = $dsnp->parse($tdb->dsn() . ",D=pdb_packer,t=prfx_");

is_deeply(pdb_packer::get_tables($dsn1), ['prfx_5'], 'get single table');
is_deeply(pdb_packer::get_tables($dsn2), ['prfx_1', 'prfx_10'], 'get many tables');
is_deeply(pdb_packer::get_tables($dsn3), [], 'get no tables');
