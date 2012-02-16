use strict;
use warnings FATAL => 'all';
use TestUtil;
use Test::More tests => 9;
use TestDB;
use DSN;
use Data::Dumper;
use ProcessLog;
$Data::Dumper::Indent = 0;
$Data::Dumper::Terse = 1;

BEGIN {
  use_ok('TableIndexes');
  my $tdb = TestDB->new();
  $tdb->clean_db();
  my $tis = TableIndexes->new($tdb->dsn());
  $tdb->use('table_indexes');

  $tdb->dbh()->do(qq|
    CREATE TABLE pk_table1 (
      id INTEGER PRIMARY KEY
    ) ENGINE=InnoDB;
  |);

  $tdb->dbh()->do(qq|
    CREATE TABLE pk_table2 (
      id INTEGER PRIMARY KEY,
      n  VARCHAR(20) NOT NULL DEFAULT '',
      UNIQUE KEY `nk` (`n`)
    ) ENGINE=InnoDB;
  |);

  $tdb->dbh()->do(qq|
    CREATE TABLE nokey_table (
      id INTEGER
    ) ENGINE=InnoDB;
  |);

  $tdb->dbh()->do(qq|
    CREATE TABLE allkeys_table (
      id INTEGER PRIMARY KEY,
      uniq_xref INTEGER,
      uniq_ts   TIMESTAMP,
      uniq_vchr VARCHAR(15),
      goober    INTEGER,
      xref_ts   TIMESTAMP,
      UNIQUE KEY `iu_vhc`    (`uniq_vchr`),
      UNIQUE KEY `iu_ts`     (`uniq_ts`),
      UNIQUE KEY `iu_xref_i` (`uniq_xref`),
      KEY        `i_goober`  (`goober`),
      KEY        `i_xref_ts` (`xref_ts`)
    ) ENGINE=InnoDB;
  |);

  ## Creates a table with some random data for us to walk.
  ## The data never changes, so, we don't need to recreate it every time.
  $tdb->rand_data(get_files_dir() . '/table_indexes_walk_data1.sql', 0,
        '-t', 'walk_data_table1', '-d', 'table_indexes', '-g', '-r', '10',
        '-c', 'id=int_pk', '-c', 'v=varchar(15)', '-s', '201103091515');

  $tdb->rand_data(get_files_dir() . '/table_indexes_walk_data2.sql', 0,
        '-t', 'walk_data_table2', '-d', 'table_indexes', '-g', '-r', '54',
        '-c', 'id=int_pk', '-c', 'v=varchar(15)', '-s', '201103091516');

  $tdb->rand_data(get_files_dir() . '/table_indexes_walk_data3.sql', 0,
        '-t', 'walk_data_table3', '-d', 'table_indexes', '-g', '-r', '54',
        '-c', 'id=int_pk', '-c', 'v=varchar(15)', '-s', '201103091517');
  eval {
    $tdb->dbh()->do(qq|INSERT INTO walk_data_table3 (id, v) VALUES(100, 'abcdefg')|);
    $tdb->dbh()->commit();
    $tdb->dbh()->{'AutoCommit'} = 0;
  };
  if($@) {
    die("$@");
  }
}

my $tdb = TestDB->new();
$::PL->logpath('pdb-test-harness');
my $dsnp = DSNParser->default();
my $tidx = new_ok('TableIndexes' => [$dsnp->parse($tdb->dsn())]);

is_deeply($tidx->sort_indexes('table_indexes', 'pk_table1'),
  [ { 'name' => 'PRIMARY', 'column' => 'id', 'key_type' => 'primary', 'column_type' => 'int' } ], 'INTEGER PRIMARY KEY');

is_deeply($tidx->sort_indexes('table_indexes', 'pk_table2'),
  [
    { 'name' => 'PRIMARY',
      'column' => 'id',
      'key_type' => 'primary',
      'column_type' => 'int'
    },
  ], 'INT PK + VCHAR UNIQUE KEY ');

eval {
  $tidx->sort_indexes('table_indexes', 'nokey_table');
};
like($@, qr/^No suitable index found.*/, 'NO KEYS');


is_deeply($tidx->sort_indexes('table_indexes', 'allkeys_table'),
  [
    { 'name' => 'PRIMARY',
      'column' => 'id',
      'key_type' => 'primary',
      'column_type' => 'int'
    },
    { 'name' => 'iu_xref_i',
      'column' => 'uniq_xref',
      'key_type' => 'unique',
      'column_type' => 'int'
    },
    { 'name' => 'iu_ts',
      'column' => 'uniq_ts',
      'key_type' => 'unique',
      'column_type' => 'timestamp'
    },
    { 'name' => 'i_goober',
      'column' => 'goober',
      'key_type' => 'key',
      'column_type' => 'int'
    },
    { 'name' => 'i_xref_ts',
      'column' => 'xref_ts',
      'key_type' => 'key',
      'column_type' => 'timestamp'
    }
  ], 'MANY KEYS');

eval {
  my $idx = $tidx->get_best_index('table_indexes', 'walk_data_table1');
  is($tidx->walk_table_base(index => $idx, size => 2, db => 'table_indexes',
                                callback => sub { diag(Dumper(\@_)); },
                                table => 'walk_data_table1'), 10, 'walks 10 rows by twos');
};
if($@) {
  diag("$@");
  fail('walks 10 rows');
}

eval {
  my $idx = $tidx->get_best_index('table_indexes', 'walk_data_table2');
  is($tidx->walk_table_base(index => $idx, size => 50, db => 'table_indexes',
                                callback => sub { diag(Dumper(\@_)); },
                                table => 'walk_data_table2'), 54, 'walks 54 rows by fifties');
};
if($@) {
  diag("$@");
  fail('walks 54 rows');
}

eval {
  my $idx = $tidx->get_best_index('table_indexes', 'walk_data_table3');
  is(($tidx->walk_table_base(index => $idx, size => 50, db => 'table_indexes',
                                  callback => sub { diag(Dumper(\@_)); },
                                  table => 'walk_data_table3'))[0], 55, 'walks 55 rows by fifties');
};
if($@) {
  diag("$@");
  fail('walks 55 rows');
}
