#!/opt/local/bin/perl
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
use warnings;

use Getopt::Long qw(:config no_ignore_case);
use Pod::Usage;
use Data::Dumper;

my $out="random_test_data.sql";
my $table="test_data";
my $database="test";
my $n_rows = 500_000;

my $seed=undef;
my $base_time=time;
my $generate_table=0;
my $table_engine="InnoDB";
my $for_infile = 0;
my %columns = ();

GetOptions(
  "h|help" => sub { pod2usage(); },
  "t|table=s" => \$table,
  "d|database=s" => \$database,
  "o|out=s" => \$out,
  "g|generate-table" => \$generate_table,
  "c|column=s" => \%columns,
  "r|rows=i" => \$n_rows,
  "e|engine=s" => \$table_engine,
  "i|for-infile" => \$for_infile,
  "s|seed=i" => \$seed,
  "T|seed-time=i" => \$base_time
);

if($seed) {
  srand($seed);
}

sub generate_varchar {
  my $length_of_randomstring=shift;# the length of 
  # the random string to generate

  my @chars=('a'..'z','A'..'Z','0'..'9','_');
  my $random_string;
  foreach (1..$length_of_randomstring) {
    # rand @chars will generate a random 
    # number between 0 and scalar @chars
    $random_string.=$chars[rand @chars];
  }
  return $random_string;
}

sub generate_integer {
  my $length=shift; # Powers of ten.
  # $length == 0 is a number between 0-9
  # == 1 is 0-19
  # == 2 is 0-99
  # == 3 is 0-199
  # etc.
  return int(rand($length**10));
}

sub generate_timestamp {
  my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) =
                                  localtime(int($base_time - rand(2**28)));
  $year += 1900;
  $mon  += 1;

  return "$year-$mon-$mday ${hour}:${min}:${sec}";
}

unless($out eq "-") {
  open SQL, ">$out";
}
else {
  *SQL=\*STDOUT
}

if($generate_table and !$for_infile) {
  print SQL "USE $database; ";
  print SQL "DROP TABLE IF EXISTS $table;\n\n";
  my $sql_columns = "";
  map {
    my $c = $_;
    my $t = $columns{$c};
    $t = "INTEGER PRIMARY KEY AUTO_INCREMENT" if($t eq 'int_pk');
    $sql_columns .= "$c $t, ";
  } sort keys %columns;
  $sql_columns =~ s/, $//;
  print SQL "CREATE TABLE IF NOT EXISTS $table ($sql_columns) ENGINE='$table_engine';\n\n";
}

my $cols_str = join(",", sort keys %columns);
foreach my $i (1..$n_rows) {
  my $vals = "";
  map {
    my $t = $columns{$_};
    if($t =~ /^int_pk/) {
      $vals .= "$i,";
    }
    elsif($t =~ /^integer\((\d+)\)/) {
      $vals .= generate_integer($1) . ",";
    }
    elsif($t =~ /^integer/) {
      $vals .= generate_integer(10) . ",";
    }
    elsif($t =~ /^timestamp/) {
      if($for_infile) {
        $vals .= generate_timestamp() . ",";
      }
      else {
        $vals .= "'". generate_timestamp() . "',";
      }
    }
    elsif($t =~ /^(?:var)?char\((\d+)\)/) {
      if($for_infile) {
        $vals .= generate_varchar($1) . ",";
      }
      else {
        $vals .= "'". generate_varchar($1) . "',";
      }
    }
  } sort keys %columns;
  $vals =~ s/,$//;
  if(!$for_infile) {
    print SQL "INSERT INTO $table ($cols_str) VALUES ($vals);\n";
  }
  else {
    $vals =~ s/,/\t/g;
    print SQL "$vals\n";
  }
}
close SQL;

__END__

=head1 NAME

gen_tbdata.pl - Generates an SQL "dump" of random data for development/testing purposes.

=head1 SYNOPSIS

gen_tbdata.pl [-h] --table <rand_table> -d <test> [-o <file.sql>] [-g]
              -c zeet=varchar(100) -c norse=timestamp --rows 500_000

Options:
    --help, -h            This help.

    --table,-t            Name of table to generate.

    --database,-d         Name of database to use (must exist).

    --out,-o              Filename ot output to, or '-' for STDOUT. Defaults to 'random_test_data.sql'.

    --generate-table,-g   Causes output to issue 'drop table', and 'create table' for the table..

    --column,-c           May be specified multiple times. Format is: column_name=type(length)
                          'id' column is automatically included, so you need not add that.
                          Supported types are:
                              varchar
                              char
                              timestamp (weird, but mostly random)
                              integer
    --rows,-r             Number of rows to generate. Can have '_' as a comma. E.g., 500_000 is 500,000.
    --engine,-e           Engine to use. Should be either MyISAM, MEMORY, or InnoDB(default).
    --for-infile,-i       Generate data suitable for LOAD DATA INFILE.
                          This disables --generate-table, --table, --database,
                          --engine, and the use of column names.
    --seed,-s             Explicitly set the random seed. Expert use only.
    --seed-time,-T        Explicitly set the base time used for random timestamp generation. Experts only.
