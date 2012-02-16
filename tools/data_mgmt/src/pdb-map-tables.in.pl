#!/usr/bin/env perl
use strict;
use warnings FATAL => 'all';

# ###########################################################################
# ProcessLog package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessLog package
# ###########################################################################

# ###########################################################################
# DSN package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End DSN package
# ###########################################################################

package pdb_map_tables;
use strict;
use warnings FATAL => 'all';
use Getopt::Long qw(:config no_ignore_case);
use List::Util;
use DBI;
use Data::Dumper;
$Data::Dumper::Indent = 0;
$Data::Dumper::Sortkeys = 1;

use ProcessLog;
use DSN;

my %o;
my $dsn;
my $pl;

my @tables;

sub main {
  @ARGV        = @_;
  %o           = ();
  $dsn         = undef;
  $pl          = undef;

  @tables      = ();
  my @children = ();
  
  my $dsnp = DSNParser->default();
  GetOptions( \%o, 'help|h', 'base-pk-col|C=s' );

  $dsn = $dsnp->parse( $ARGV[0] );
  $pl = ProcessLog->new($0, "$0.log");
  
  $pl->d("Options:", Dumper(\%o));
  
  @tables = map { $_->[0] } @{$dsn->get_dbh(1)->selectall_arrayref("SHOW TABLES FROM `". $dsn->get('D') ."`")};
  @children = map_table($dsn->get('t'), $o{'base-pk-col'});
  print($dsn->get('t'), ":\n");
  foreach my $chld (@children) {
    print("  ", $chld->[0], " (", join(",", @{$chld->[1]}), ")", "\n");
  }
  return 0;
}

sub map_table {
  my ( $table, $pref_pk_col ) = @_;
  my @children;
  if ( !$pref_pk_col ) {
    for (qw(${table}_id id)) {
      ($pref_pk_col) = grep( /$_/, map { $_->{'Field'} } @{ get_columns($table) } );
      last if ($pref_pk_col);
    }
    if ( !$pref_pk_col ) {
      die("Unable to find likely PK column");
    }
  }
  
  foreach my $tbl (@tables) {
    my @matched_cols = ();
    next if($tbl eq $table);
    @matched_cols = grep(/^$pref_pk_col$/, map { $_->{'Field'} } @{get_columns($tbl)});
    push(@children, [$tbl, [@matched_cols]]) if(@matched_cols);
  }
  return @children;
}

sub get_columns {
  my ($table_name) = @_;
  my $dbh = $dsn->get_dbh(1);
  my $cols;

  my $sql = "SHOW COLUMNS FROM `" . $dsn->get('D') . "`.`$table_name`";
  $pl->d( "SQL:", $sql );
  $cols = $dbh->selectall_arrayref( $sql, { Slice => {} } );
  $pl->d("Columns:", Dumper([ map { $_->{'Field'} } @$cols ]));
  return $cols;
}

if ( !caller ) { exit( main(@ARGV) ); }

1;
