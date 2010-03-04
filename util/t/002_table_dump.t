use Test::Moretests => 2;
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
