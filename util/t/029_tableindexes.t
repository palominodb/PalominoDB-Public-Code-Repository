use strict;
use warnings FATAL => 'all';
use TestUtil;
use Test::More tests => 6;
use TestDB;
use DSN;

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
}

my $tdb = TestDB->new();
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
