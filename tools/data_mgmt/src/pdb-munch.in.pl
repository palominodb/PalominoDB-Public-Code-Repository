#!/usr/bin/env perl
# Copyright (c) 2009-2010, PalominoDB, Inc.
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
# ProcessLog package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessLog package
# ###########################################################################

# ###########################################################################
# IniFile package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End IniFile package
# ###########################################################################

# ###########################################################################
# DSN package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End DSN package
# ###########################################################################

# ###########################################################################
# TableIndexes package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End TableIndexes package
# ###########################################################################

#package MunchSpec;
#use strict;
#use warnings FATAL => 'all';
#use IniFile;
#
#sub new {
#  my ($class, $specfile) = @_;
#  my $self = {};
#  $self->{spec} = IniFile::read_config($specfile);
#  $self->{specfile} = $specfile;
#  bless $self, $class;
#  
#  # Verify each datatype in the spec.
#  foreach my $type (keys %{$self->{spec}}) {
#    for my $c (qw(source column-type method)) {
#      if(not defined($self->{spec}->{$type}->{$c})) {
#        die("$c is required for all data specs");
#      }
#    }
#  }
#  
#  return $self;
#}
#
#sub data_matches {
#  my ($self, $type, $data) = @_;
#  my $spec = $self->{spec}->{$type};
#  die("Uknown datatype $type") if(not defined $spec);
#  my @matches = grep /match\d+/, sort keys %$spec;
#  
#}
#
#1;

package pdb_munch;
use strict;
use warnings FATAL => 'all';

use Getopt::Long qw(:config no_ignore_case);
use Text::CSV_XS;
use Data::Dumper;
use DBI;
use Pod::Usage;

use ProcessLog;
use IniFile;
use DSN;
use TableIndexes;

my $default_spec =<<'EOF';
; This is the built-in spec provided with pdb-munch.
; All of the source values have been commented out, and you MUST
; uncomment them and fill them in with real values.

[name]
column-type = varchar
method      = roundrobin
; Uncomment the below if you've got a CSV of firstname,lastname
;source    = csv:names.csv
match1      = (\w+) (\w+)

[phonenumber]
column-type = varchar
method      = random
; Uncomment the below if you've got a CSV of phone numbers
;source     = csv:phone_numbers.csv

; These matches are in descreasing priority
; The capture group specifies which portion of the number
; to replace with seed data.
match1      = \d{3}-(\d{3}-\d{4})
match2      = \(\d{3}\) (\d{3}-\d{4})
match3      = \+?\d{1,3} \d{3} (\d{3} \d{4})
match4      = \+?\d{1,3} \d{3} (\d{3}-\d{4})

[email]
column-type = varchar
method = random
;source = random
; Assumes a pre-sanitized email address
match1 = (.*?)@.*

[address_line_one]
column-type = varchar
method      = random
;source      = csv:addresses_line_one.csv
method      = random

match1     = \s*(\d+) ([a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ?).*
match2     = \s*(\d+) ([a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ?).*
match3     = \s*(\d+) ([a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ?).*
match4     = \s*(\d+) ([a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ?).*
EOF

my %spec;
my %conf;
my $pl;
my $cur_tbl;
my $db;
my $dsn;

my $rr_upd;

sub main {
  @ARGV = @_;
  my %c = ('logfile' => "$0.log", 'batch-size' => 10_000);
  my $dsnp = DSNParser->default();
  my $tbl_indexer;
  %spec = ();
  %conf = ();
  $cur_tbl = undef;
  $dsn = undef;
  $db = undef;
  $rr_upd = 0;
  GetOptions(\%c,
    'help|h',
    'i-am-sure',
    'logfile|L=s',
    'dry-run|n',
    'dump-spec',
    'spec|s=s',
    'config|c=s',
    'batch-size|b=i',
  );
  
  if($c{'help'}) {
    pod2usage(-verbose => 99);
    return 1;
  }
  
  $pl = ProcessLog->new($0, $c{logfile});
  
  ## Dump the spec and exit, if requested.
  if($c{'dump-spec'}) {
    my $spec_fh;
    open($spec_fh, ">default_spec.conf") or die("Unable to open default_spec.conf for writing");
    print($spec_fh $default_spec);
    close($spec_fh);
    $pl->i("Dumped default spec to default_spec.conf");
    return 0;
  }
  
  if(not exists $c{spec}) {
    $pl->e("--spec required. Try --help.");
    return 1;
  }
  
  if(not exists $c{config}) {
    $pl->e("--config required. Try --help.");
    return 1;
  }
  
  ## Load the spec file into a hash
  %spec = IniFile::read_config($c{spec});
  if(not %spec) {
    $pl->e("Unable to load $c{spec}.");
    return 1;
  }
  
  ## Verify each datatype in the spec.
  foreach my $type (keys %spec) {
    for my $c (qw(source column-type method)) {
      if(not defined($spec{$type}->{$c})) {
        $pl->e("$c is required for all data specs");
        return 1;
      }
    }
  }
  
  ## Load all the CSV and List data sources into the spec, directly.
  foreach my $type (keys %spec) {
    my $src = $spec{$type}->{source};
    $pl->d("src:", $src);
    if($src =~ /^csv:(.*)/) {
      my $fh;
      my $csv = Text::CSV_XS->new({binary => 1});
      unless(open($fh, "<$1")) {
        $pl->e("Unable to open seed data: $1");
        return 1;
      }
      ## Assuming that the data is basically hand-generated and thus
      ## will not be gigantic.
      while(my $row = $csv->getline($fh)) {
        push( @{$spec{$type}->{data}}, $row );
      }
    }
    elsif($src =~ /^list:(.*)/) {
      $spec{$type}->{data} = [map { [$_] }split(/,/, $1)];
    }
  }
  
  ## Load the config file into a hash
  %conf = IniFile::read_config($c{config});
  if(not %conf) {
    $pl->e("Unable to load $c{config}");
    return 1;
  }
  
  $pl->d("Spec:", Dumper(\%spec));
  $pl->d("Config:", Dumper(\%conf));
  
  ## Get connection information out of the config file
  $dsn = $dsnp->parse($conf{connection}{dsn});
  $db  = $dsn->get('D');
  delete $conf{connection};
  $tbl_indexer = TableIndexes->new($dsn);
  

  foreach my $tbl (sort keys %conf) {
    $rr_upd = 0;
    $cur_tbl = $tbl;
    $pl->d("Table config:", $conf{$tbl});
    $tbl_indexer->walk_table(undef, $c{'batch-size'}, \&update_row, $db, $tbl, $c{'dry-run'});
  }
  
  return 0;
}

sub generate_varchar {
  # the length of the random string to generate
  my $length_of_randomstring=shift;
  $length_of_randomstring ||= 5;

  my @chars=('a'..'z','A'..'Z','0'..'9','_');
  my $random_string;
  foreach (1..$length_of_randomstring) {
    # rand @chars will generate a random 
    # number between 0 and scalar @chars
    $random_string.=$chars[rand @chars];
  }
  return $random_string;
}

## Does the actual work of updating rows to have obfuscated values.
sub update_row {
  my ($idx_col, $dbh, $min_idx, $max_idx, $row, $dry_run) = @_;
  my $tbl_config = $conf{$cur_tbl};
  my (@vals, @data);
    
  $pl->d("Row:", Dumper($row));
  $pl->d("SQL:", "UPDATE `$db`.`$cur_tbl` SET ". join("=?, ", sort keys %$tbl_config) ."=? WHERE `$idx_col`=?");
  
  ## The keys are sorted here to force the same order in the query as in @vals
  ## Since, @vals is passed wholesale onto $sth->execute() later.
  my $sth = $dbh->prepare_cached("UPDATE `$db`.`$cur_tbl` SET ". join("=?, ", sort keys %$tbl_config) ."=? WHERE `$idx_col`=?");
  foreach my $col (sort keys %$tbl_config) {
    
    ## Populate the @data array with either seed data from the pre-loaded CSV file
    ## Or a couple values from a random string generator.
    if($spec{$$tbl_config{$col}}->{source} eq "random") {
      @data = ( [generate_varchar(int(rand(length($row->{$col}))))],
      [generate_varchar(int(rand(length($row->{$col}))))],
      [generate_varchar(int(rand(length($row->{$col}))))] );
    }
    else {
      @data = @{$spec{$$tbl_config{$col}}->{data}};  
    }
    
    ## Select the data in the fashion requested.
    if($spec{$$tbl_config{$col}}->{method} eq 'random') {
      push @vals, $data[int(rand($#data))];
    }
    elsif($spec{$$tbl_config{$col}}->{method} eq 'roundrobin') {
      push @vals, $data[ $rr_upd % $#data ];
      $rr_upd++;
    }
    
    ## Keys are sorted here so that each of the matchN keys is in ascending order
    ## thus prioritising lower values of N.
    SEED_KEY: foreach my $sk (sort(grep(/match\d+/, keys(%{$spec{$$tbl_config{$col}}}))) ) {
      my $rgx = $spec{$$tbl_config{$col}}{$sk};
      my @res = $row->{$col} =~ /^$rgx$/;
      $pl->d("R:", qr/^$rgx$/, $#res+1, @res);
      if(@res) {
        for(my $i=0; $i < $#res+1; $i++) {
          $pl->d("S:", $res[$i], "(", @{$vals[-1]}, ")", "*", $vals[-1]->[$i], "*", $i, scalar @{$vals[-1]});
          $row->{$col} =~ s/$res[$i]/$vals[-1]->[$i]/;
        }
        $vals[-1] = $row->{$col};
        last SEED_KEY;
      }
    }
  }
  $pl->d("SQL Bind:", @vals, $row->{$idx_col});
  $sth->execute(@vals, $row->{$idx_col}) unless($dry_run);
}

if(!caller) { exit(main(@ARGV)); }

=pod

=head1 NAME

pdb-munch - Flexible data obfuscation tool

=head1 SYNOPSIS

This tool was made to santize records in a table so that
taking it out of the secure environment in which it was created
is feasible. This can be useful for devs who want a copy of "real"
data to take home with them for testing purposes.

This tool is designed to modify your data. B<DO NOT> run it on
production systems. Because it does destructive operations on your
data, it does not accept hostnames on the commandline. Always double
and triple check your configuration before running this tool.

=head1 OPTIONS

=over 8

=item --i-am-sure

Do not prompt to continue after displaying the processed configuration.

Use this option at your own peril.

=item --dry-run,-n

Only report on what actions would be taken.

=item --dump-spec

Dump the built-in spec file to the file F<default_spec.cnf>.

This is a good starting point for building your own spec file.

=item --spec,-s

Use the column types from this file.

=item --config,-c

Use the host/table configuration in this file.

=item --batch-size,-b

Tells pdb-munch to modify --batch-size records at a time.

Default: 10,000

=back

=head1 EXAMPLES

  # Munch the data on test_machine using
  # The default column type specs.
  pdb-munch -d default_spec.conf -c test_machine.conf
  
  # Dumps the built-in spec to default_spec.conf
  # This is a very handy starting point, since the default
  # munch spec includes several datatypes. 
  pdb-munch --dump-spec
  
=head1 SPEC FILES

Spec files describe different kinds of datatypes stored inside
a mysql column (for instance, an address stored in a varchar column).
They also describe how to modify the data contained in those columns.

A spec file is an Ini style configuration file, composed of one or more
sections following the form:

  [<datatype>]
  column-type = <mysql column type>
  source      = <csv:<file>|list:<comma separated list>|random>
  match<I>    = <perl regex>
  match<I+1>  = <perl regex>
  match<I+N>  = <perl regex>

=head1 ENVIRONMENT

Like all PalominoDB data management tools, this tool responds to the
environment variable C<Pdb_DEBUG>, when it is set, this tool produces
copious amounts of debugging output.

=cut

1;