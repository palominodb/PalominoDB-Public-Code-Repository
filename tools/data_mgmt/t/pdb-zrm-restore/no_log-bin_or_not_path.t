use strict;
use warnings FATAL => 'all';
use Test::More tests => 3;
use TestUtil;
use TestDB;
use File::Basename;
use File::Path qw(make_path remove_tree);

require_ok('src/pdb-zrm-restore.in.pl');

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

ok(pdb_zrm_restore::main(@opts) == 0, 'runs correctly with log-bin not a path');

system("cp", $ENV{PDB_SANDBOX_CNF}, "$ENV{PDB_SANDBOX_CNF}.bak");
system("sed -i -e '/log-bin/d ; a\\ log-bin = ". dirname($ENV{PDB_SANDBOX_CNF}) ."/binlogs' $ENV{PDB_SANDBOX_CNF}");
make_path(dirname($ENV{PDB_SANDBOX_CNF}) ."/binlogs");

ok(pdb_zrm_restore::main(@opts) == 0, 'runs correctly with log-bin a path');

END {
  $tdb->stop();
  remove_tree(dirname($ENV{PDB_SANDBOX_CNF}) ."/binlogs");
  rename("$ENV{PDB_SANDBOX_CNF}.bak", $ENV{PDB_SANDBOX_CNF});
  $tdb->start();
}
