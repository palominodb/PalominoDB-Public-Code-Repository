# 003_table_partition_maxvalue.t
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

use Test::More tests => 23;
use ProcessLog;
use TablePartitions;
use TestDB;

my $tdb = TestDB->new;
$tdb->clean_db;
$tdb->use('test');
$tdb->dbh->do(
  qq#CREATE TABLE tbmx_date (
    id integer auto_increment,
    ts datetime not null,
    v  varchar(22) not null,
    primary key (id,ts)
  )
  PARTITION BY RANGE (to_days(ts)) (
    partition p0 values less than (to_days('2010-01-01')),
    partition p1 values less than (to_days('2010-01-02')),
    partition p2 values less than (to_days('2010-01-03')),
    partition p3 values less than MAXVALUE
  )
#);
$tdb->dbh->do(
  qq#CREATE TABLE tp (
    id integer auto_increment,
    ts datetime not null,
    v  varchar(22) not null,
    primary key (id,ts)
  )
  PARTITION BY RANGE (id) (
    partition p0 values less than (100),
    partition p1 values less than MAXVALUE
  )
#);
my $pl = ProcessLog->null;
$pl->quiet(1);
my $tp = TablePartitions->new($pl, $tdb->dbh, 'test', 'tbmx_date');
my $tp2 = TablePartitions->new($pl, $tdb->dbh, 'test', 'tp');

ok($tp, 'instantiation check');
my $parts=  [
  {'description' => 734138,
    'name' => 'p0',
    'position' => 1,
    'sub_name' => undef,
    'sub_position' => undef
  },
  {'description' => 734139,
    'name' => 'p1',
    'position' => 2,
    'sub_name' => undef,
    'sub_position' => undef
  },
  {'description' => 734140,
    'name' => 'p2',
    'position' => 3,
    'sub_name' => undef,
    'sub_position' => undef
  },
  {'description' => 'MAXVALUE',
    'name' => 'p3',
    'position' => 4,
    'sub_name' => undef,
    'sub_position' => undef
  }
];
is_deeply($tp->partitions(), $parts, 'partitions');
is_deeply($tp->first_partition, $parts->[0], 'first partition');
is_deeply($tp->last_partition, $parts->[-1], 'last partition');
is($tp->method, 'RANGE', 'method');
is($tp->has_maxvalue_data, 0, 'empty table');
$tdb->dbh->do(qq#INSERT INTO tbmx_date (ts,v) VALUES ('2010-01-04', 'little test')#);
is($tp->has_maxvalue_data, 1, 'maxvalue populated');

$tp->drop_partition('p0', 0);
my $first_part = shift @$parts;
is_deeply($tp->partitions(), $parts, 'partition dropped');

is($tp->add_range_partition('p4', '2010-01-05', 0), undef, "don't add after maxvalue");

ok($tp->start_reorganization('p3'), 'start re-organization');
ok($tp->add_reorganized_part('p3', '2010-01-04'), 'add reorg part p3');
ok($tp->add_reorganized_part('p4', '2010-01-05'), 'add reorg part p4');
ok($tp->add_reorganized_part('p5', 'MAXVALUE'), 'add reorg part p5');
$pl->quiet(0);
ok($tp->end_reorganization(0), 'finish re-organization');
$pl->quiet(1);

$parts = [
  {'description' => 734139,
    'name' => 'p1',
    'position' => 1,
    'sub_name' => undef,
    'sub_position' => undef
  },
  {'description' => 734140,
    'name' => 'p2',
    'position' => 2,
    'sub_name' => undef,
    'sub_position' => undef
  },
  {'description' => 734141,
    'name' => 'p3',
    'position' => 3,
    'sub_name' => undef,
    'sub_position' => undef
  },
  {'description' => 734142,
    'name' => 'p4',
    'position' => 4,
    'sub_name' => undef,
    'sub_position' => undef
  },
  {'description' => 'MAXVALUE',
    'name' => 'p5',
    'position' => 5,
    'sub_name' => undef,
    'sub_position' => undef
  }
];

is_deeply($tp->partitions(), $parts, 'partitions after re-org');
is($tp->desc_from_datelike('p1'), '2010-01-02', 'desc_from_datelike');

is($tp->expression_column, 'ts', 'datelike expression column');
is($tp2->expression_column, 'id', 'non-datelike expression column');

ok($tp2->start_reorganization('p1'), 'start re-organization 2');
ok($tp2->add_reorganized_part('p1', 200), 'add reorg part p1 2');
ok($tp2->add_reorganized_part('p2', 300), 'add reorg part p2 2');
ok($tp2->add_reorganized_part('p3', 'MAXVALUE'), 'add reorg part p3 2');
$pl->quiet(0);
ok($tp2->end_reorganization(0), 'finish re-organization 2');
$pl->quiet(1);
