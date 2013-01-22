# 023_table_age.t
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
