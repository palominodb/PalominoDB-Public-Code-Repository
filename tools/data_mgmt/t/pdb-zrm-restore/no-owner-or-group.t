use strict;
use warnings FATAL => 'all';
use Test::More tests => 2;
use TestUtil;
use TestDB;
use File::Basename;
use File::Path qw(make_path remove_tree);

require_ok('src/pdb-zrm-restore');

my $tdb = new TestDB;
my $backups = $ENV{PDB_CODE_ROOT} ."/tools/data_mgmt/t/pdb-zrm-restore/20100512025732";
my @opts = (
               "--log-file=pdb-test-harness",
               "--rel-base", "$ENV{PDB_CODE_ROOT}/tools/data_mgmt",
               "--create-dirs", "--force",
               "--defaults-file", $ENV{PDB_SANDBOX_CNF},
               "--mysqld", "$TestDB::cnf->{'mysqld'}->{'basedir'}/bin/mysqld_safe",
               $backups
            );
$tdb->stop();
$tdb->fsclear();

ok(pdb_zrm_restore::main(@opts) == 0, 'runs correctly without owner or group set in my.cnf');

END {
  $tdb->start();
}
