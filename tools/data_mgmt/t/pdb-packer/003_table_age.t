use strict;
use warnings FATAL => 'all';
use Test::More tests => 3;
use TestDB;
use DateTime;

my $now = DateTime->now( time_zone => 'local');
BEGIN {
  require_ok('src/pdb-packer.in.pl');
  my $tdb = TestDB->new();
  $tdb->clean_db();
  $tdb->use('pdb_packer_age');
  $tdb->dbh()->do(qq|
    CREATE TABLE tzbr_20100417 (
      `id` INTEGER,
      `va` VARCHAR(5)
    )
    |);
}

my $tdb = TestDB->new();
my $dsnp = DSNParser->default();
$dsnp->add_key('r', { 'desc' => 'Table prefix', 'mandatory' => 0 });
my $dsn = $dsnp->parse($tdb->dsn() . ",D=pdb_packer_age,r=tzbr,t=tzbr_20100417");

my $af = $pdb_packer::age_format;
$pdb_packer::age_format = 'createtime';

isa_ok(
  pdb_packer::table_age($dsn),
  'DateTime'
);

is_deeply(
  pdb_packer::table_age($dsn),
  $now,
  'createtime age'
);

$pdb_packer::age_format = $af;

is_deeply(
  pdb_packer::table_age($dsn),
  DateTime->new(
    year => 2010,
    month => 04,
    day => 17
  ),
  'formatted age'
);
