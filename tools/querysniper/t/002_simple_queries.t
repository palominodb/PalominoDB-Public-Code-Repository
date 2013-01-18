# 002_simple_queries.t
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

package mk_loadavg;
1;
use strict;
use warnings FATAL => 'all';
use Test::More;
use QuerySniper;

my $qr = QueryRules->new;
ok($qr->load('t/simple.rules'), 'load simple rules');
ok($qr->compile, 'compile rules');
my $i=2;

my @queries = (
  {
    d => qq|Kill for 'kill Time > 10'|,
    q => {
      Id => 0,
      Time => 11,
      Db => 'test',
      User => 'Randy',
      Host => 'localhost',
      Command => 'Query',
      State => 'Sending data',
      Info => 'SELECT * FROM big_table'
    },
    r => 0
  },
  {
    d => qq|Pass for 'pass Time > 20'|,
    q => {
      Id => 0,
      Time => 21,
      Db => 'test',
      User => 'Randy',
      Host => 'localhost',
      Command => 'Query',
      State => 'Sending data',
      Info => 'SELECT * FROM big_table'
    },
    r => 1
  },
  {
    d => qq|Kill for 'kill User == 'Fred''|,
    q => {
      Id => 0,
      Time => 1,
      Db => 'test',
      User => 'Fred',
      Host => 'localhost',
      Command => 'null',
      State => 'null',
      Info => 'null'
    },
    r => 0
  },
  {
    d => qq|Pass for 'pass User == 'Bob''|,
    q => {
      Id => 0,
      Time => 1,
      Db => 'test',
      User => 'Bob',
      Host => 'localhost',
      Command => 'null',
      State => 'null',
      Info => 'null'
    },
    r => 1
  },
  { 
    d => qq|Kill for 'kill Db == 'null''|,
    q => {
      Id => 0,
      Time => 1,
      Db => 'null',
      User => 'Bob',
      Host => 'localhost',
      Command => 'null',
      State => 'null',
      Info => 'null'
    },
    r => 0
  },
  { 
    d => qq|Pass for 'pass Host == 'special''|,
    q => {
      Id => 0,
      Time => 15,
      Db => 'null',
      User => 'Fred',
      Host => 'special',
      Command => 'null',
      State => 'null',
      Info => 'null'
    },
    r => 1
  },
);

foreach my $tq (@queries) {
  is($qr->run($tq->{q}), $tq->{r}, $tq->{d});
  $i++;
}

done_testing($i);
