# 004_table_rotate.t
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
