# 002_table_dump.t
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

use Test::More tests => 2;
use DBI;
use ProcessLog;
use TableDumper;
use TestDB;

my $tdb = TestDB->new;
$tdb->clean_db;
$tdb->use('dump');

$tdb->dbh->do(
  qq#
  CREATE TABLE little_dump (
    id integer auto_increment,
    ts datetime not null,
    v varchar(10) not null,
    primary key (id,ts)
  )
  #);

$tdb->dbh->do(
  qq#
  INSERT INTO little_dump (ts,v)
    VALUES
    ('2010-01-01', 'aaaaaaaaaa'),
    ('2010-01-02', 'bbbbbbbbbb'),
    ('2010-01-03', 'cccccccccc'),
    ('2010-01-04', 'dddddddddd'),
    ('2010-01-05', 'eeeeeeeeee')
  #);

my $pl = ProcessLog->null;
$pl->quiet(1);
my $td = TableDumper->new($tdb->dbh, $pl, $tdb->user, $tdb->password);
