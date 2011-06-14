#!/usr/bin/env perl
# Copyright (c) 2009-2011, PalominoDB, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#
#   * Neither the name of PalominoDB, Inc. nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
use strict;
use warnings FATAL => 'all';

# ###########################################################################
# ProcessLog package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessLog package
# ###########################################################################

# ###########################################################################
# DSN package FSL_VERSION
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
