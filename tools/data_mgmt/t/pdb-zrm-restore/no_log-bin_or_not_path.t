# no_log-bin_or_not_path.t
# Copyright (C) 2013 PalominoDB, Inc.
# 
# You may contact the maintainers at eng@palominodb.com.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

use strict;
use warnings FATAL => 'all';
use Test::More tests => 3;
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
