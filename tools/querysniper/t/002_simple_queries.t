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
