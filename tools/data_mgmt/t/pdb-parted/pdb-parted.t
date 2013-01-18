#!/usr/bin/perl
# pdb-parted.t - crespucule
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
use Test::More tests => 25;
BEGIN {
  $ENV{Pdb_DEBUG} = 1;
}
use ProcessLog;
use DateTime::Duration;
use TestDB;
use TestUtil;
use Data::Dumper;
{
  no warnings 'once';
  $Data::Dumper::Indent = 0;
  $Data::Dumper::SortKeys = 1;
}

require_ok('src/pdb-parted');

BEGIN {
  my $tdb = TestDB->new();
  $tdb->clean_db();

  # Create remote server tables.
  $tdb->use('remote_pdb_parted');
  $tdb->dbh()->do(qq|
CREATE TABLE test_table_3d (
  ts DATE NOT NULL PRIMARY KEY
)
PARTITION BY RANGE( TO_DAYS(ts) ) (
  PARTITION p0 VALUES LESS THAN ( TO_DAYS('2011-01-15') )
);
|);

  # Create local tables
  $tdb->use('pdb_parted');
  $tdb->dbh()->do(qq|
CREATE TABLE test_table_3d (
  ts DATE NOT NULL PRIMARY KEY
)
PARTITION BY RANGE( TO_DAYS(ts) ) (
  PARTITION p0 VALUES LESS THAN ( TO_DAYS('2011-01-15') ),
  PARTITION p1 VALUES LESS THAN ( TO_DAYS('2011-01-16') ),
  PARTITION p2 VALUES LESS THAN ( TO_DAYS('2011-01-17') )
);
|);
  $tdb->dbh()->do(qq|
CREATE TABLE test_table_3w (
  ts DATE NOT NULL PRIMARY KEY
)
PARTITION BY RANGE( TO_DAYS(ts) ) (
  PARTITION p0 VALUES LESS THAN ( TO_DAYS('2011-01-14') ),
  PARTITION p1 VALUES LESS THAN ( TO_DAYS('2011-01-21') ),
  PARTITION p2 VALUES LESS THAN ( TO_DAYS('2011-01-28') )
);
|);
  $tdb->dbh()->do(qq|
CREATE TABLE test_table_3wMV (
  ts DATE NOT NULL PRIMARY KEY
)
PARTITION BY RANGE( TO_DAYS(ts) ) (
  PARTITION p0 VALUES LESS THAN ( TO_DAYS('2011-01-14') ),
  PARTITION p1 VALUES LESS THAN ( TO_DAYS('2011-01-21') ),
  PARTITION p2 VALUES LESS THAN ( TO_DAYS('2011-01-28') ),
  PARTITION p3 VALUES LESS THAN ( MAXVALUE )
);
|);
  $tdb->dbh()->do(qq|
INSERT INTO test_table_3wMV (ts) VALUES ('2011-01-29'), ('2011-01-30');
|);

}

my %o = ( prefix => 'p', interval => 'd' );

$::PL->logpath('pdb-test-harness');
my @parts;
my $tdb   = TestDB->new();
my $end  = DateTime->new( year => 2011, month => 1, day => 20 );

my $dsn  = DSNParser->default()->parse($tdb->dsn() . ",D=pdb_parted,t=test_table_3d");
my $parts = TablePartitions->new($::PL, $dsn);

my $remote_dsn;
my $remote_parts;

ok(@parts = pdb_parted::add_partitions($dsn, $parts, $end, %o), '(days) add_partitions() claims success');

$parts = TablePartitions->new($::PL, $dsn);

is($parts->last_partition()->{name}, 'p5', '(days) found expected partition');
is_deeply(
  [map { $_->{date} } @parts],
  [ map { DateTime->new(year => 2011, month => 1, day => $_) } (18,19,20) ],
  '(days) found expected dates');

# setup for test 2
$o{'interval'} = 'w';
@parts = ();
$dsn   = DSNParser->default()->parse($tdb->dsn() . ",D=pdb_parted,t=test_table_3w");
$end   = DateTime->new( year => 2011, month => 1, day => 28 )->add(weeks => 3);
$parts = TablePartitions->new($::PL, $dsn);

ok(@parts = pdb_parted::add_partitions($dsn, $parts, $end, %o), '(weeks) add_partitions() claims success');
$parts = TablePartitions->new($::PL, $dsn);

is($parts->last_partition()->{name}, 'p5', '(weeks) found expected partition');
is_deeply(
  [ map { $_->{date} } @parts ],
  [ map { DateTime->new(year => 2011, month => 2, day => $_) } (4,11,18) ],
  '(weeks) found expected dates');


# limiting number partition adds
@parts = ();
$o{'interval'} = 'd';
$o{'limit'} = 3;
$dsn  = DSNParser->default()->parse($tdb->dsn() . ",D=pdb_parted,t=test_table_3d");
$parts = TablePartitions->new($::PL, $dsn);
$end  = DateTime->new( year => 2011, month => 1, day => 28 );

ok(@parts = pdb_parted::add_partitions($dsn, $parts, $end, %o), '(days-limit) add_partitions() claims success');
$parts = TablePartitions->new($::PL, $dsn);

is($parts->last_partition()->{name}, 'p8', '(days-limit) found expected partition');
is_deeply(
  [ map { $_->{date} } @parts ],
  [ map { DateTime->new(year => 2011, month => 1, day => $_) } (21,22,23) ],
  '(days-limit) found expected dates');

@parts = ();
$o{'interval'} = 'd';
$o{'limit'} = 0;
$dsn  = DSNParser->default()->parse($tdb->dsn() . ",D=pdb_parted,t=test_table_3d");
$parts = TablePartitions->new($::PL, $dsn);
$end  = DateTime->new( year => 2011, month => 1, day => 18 );

ok(@parts = pdb_parted::drop_partitions($dsn, undef, $parts, $end, %o), '(days) drop_partitions() claims success');
$parts = TablePartitions->new($::PL, $dsn);

is($parts->first_partition()->{name}, 'p3', '(days) found expected first partition');
is_deeply(
  [ map { $_->{date} } @parts ],
  [ map { DateTime->new(year => 2011, month => 1, day => $_) } (15,16,17) ],
  '(days) found expected dates');


# Test remote archiving
@parts = ();
$o{'interval'} = 'd';
$o{'limit'} = 0;
$o{'archive'} = 1;
$dsn  = DSNParser->default()->parse($tdb->dsn() . ",D=pdb_parted,t=test_table_3d");
$remote_dsn  = DSNParser->default()->parse($tdb->dsn() . ",D=remote_pdb_parted,t=test_table_3d");
$parts = TablePartitions->new($::PL, $dsn);
$end  = DateTime->new( year => 2011, month => 1, day => 23 );

ok(@parts = pdb_parted::drop_partitions($dsn, $remote_dsn, $parts, $end, %o), '(days) drop_partitions() claims success');
$remote_parts = TablePartitions->new($::PL, $remote_dsn);

is($remote_parts->first_partition()->{name}, 'p0', '(days) found expected first partition');
is_deeply(
  [ map { $_->{date} } @parts ],
  [ map { DateTime->new(year => 2011, month => 1, day => $_) } (18,19,20,21,22) ],
  '(days) found expected dates');


# setup for test weeks-mv
@parts = ();
$o{'interval'} = 'w';
$dsn   = DSNParser->default()->parse($tdb->dsn() . ",D=pdb_parted,t=test_table_3wMV");
$end   = DateTime->new( year => 2011, month => 2, day => 18 )->add(weeks => 1);
$parts = TablePartitions->new($::PL, $dsn);

eval {
  ok(@parts = pdb_parted::add_partitions($dsn, $parts, $end, %o), '(weeks-mv) add_partitions() claims success');
};
diag('(weeks-mv) eval:', $@);
pass('(weeks-mv) add_partitions() dies with MAXVALUE data and no --i-am-sure') if($@);

$parts = TablePartitions->new($::PL, $dsn);

is($parts->last_partition()->{name}, 'p3', '(weeks-mv) found expected partition');
is_deeply(
  [ map { $_->{date} } @parts ],
  [ ],
  '(weeks-mv) found expected (no) dates');


# setup for test weeks-mv
@parts = ();
$o{'interval'} = 'w';
$o{'i-am-sure'} = 1;
$dsn   = DSNParser->default()->parse($tdb->dsn() . ",D=pdb_parted,t=test_table_3wMV");
$end   = DateTime->new( year => 2011, month => 2, day => 18 )->add(weeks => 1);
$parts = TablePartitions->new($::PL, $dsn);

eval {
  ok(@parts = pdb_parted::add_partitions($dsn, $parts, $end, %o), '(weeks-mv2) add_partitions() claims success');
};
diag('(weeks-mv) eval:', $@);
pass('(weeks-mv) add_partitions() does not die()') if(!$@);

$parts = TablePartitions->new($::PL, $dsn);

is($parts->last_partition()->{name}, 'p7', '(weeks-mv) found expected partition');
is_deeply(
  [ map { $_->{date} } @parts ],
  [ (map { DateTime->new(year => 2011, month => 2, day => $_) } (4,11,18,25)), 'MAXVALUE' ],
  '(weeks-mv) found expected dates');


# final "all the way through" test:
@parts = ();
$dsn = DSNParser->default()->parse($tdb->dsn() . ",D=pdb_parted,t=test_table_3d");
my @args = ('--logfile=pdb-test-harness', '--add', '--interval', 'd', '+1d',  $dsn->str());

ok(pdb_parted::main(@args) == 0, "pdb_parted::main() go-through");

$parts = TablePartitions->new($::PL, $dsn);
my $last_date = pdb_parted::to_date($parts->desc_from_datelike($parts->last_partition()->{name}));
ok($last_date >= DateTime->today()->add(days => 1), 'added partitions up to today+1');
