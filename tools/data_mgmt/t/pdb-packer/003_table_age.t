# 003_table_age.t
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
use TestDB;
use DateTime;

my $now = DateTime->now( time_zone => 'local');
BEGIN {
  require_ok('src/pdb-packer');
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
my $dsn = $dsnp->parse($tdb->dsn() . ',D=pdb_packer_age,r=tzbr(_\d+),t=tzbr_20100417');

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
