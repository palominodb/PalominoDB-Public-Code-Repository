#!/usr/bin/env perl
# gen_tbdata.pl
# Copyright (C) 2009-2013 PalominoDB, Inc.
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

use strict;
use warnings FATAL => 'all';

use Getopt::Long qw(:config no_ignore_case);
use Pod::Usage;
use Data::Dumper;
use POSIX qw(floor);

my $out="random_test_data.sql";
my $table="test_data";
my $database="test";
my $n_rows = 500_000;

my $seed=undef;
my $base_time=time;
my $generate_table=0;
my $table_engine="InnoDB";
my $for_infile = 0;
my $insert_ignore = 0;
my %columns = ();
my $pk_start = 1;

# If the words() type is used
# This contains the contents of /usr/share/dict/words
my @dict;

# For stateful generators
my %gen = ();

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
  "T|seed-time=i" => \$base_time,
  "P|primary-key-start=i" => \$pk_start,
  "I|insert-ignore" => \$insert_ignore
);

if($seed) {
  srand($seed);
}

sub generate_words {
  my $number_of_words=shift;
  my $str;
  for(1..$number_of_words) {
    $str .= " ". $dict[rand @dict];
  }
  return $str;
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
  return int(rand(10**$length));
}

sub generate_timestamp {
  my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) =
                                  localtime(int($base_time - rand(2**28)));
  $year += 1900;
  $mon  += 1;

  return "$year-$mon-$mday ${hour}:${min}:${sec}";
}

sub generate_linear_timestamp {
  my $sway = shift || 300;
  my $baset = $base_time;
  my $grator = sub {
    my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) =
                                localtime(int($baset += rand($sway)));
    $year += 1900;
    $mon  += 1;

    return "$year-$mon-$mday ${hour}:${min}:${sec}";
  };
  return $grator;
}

sub generate_linear_integer {
  my $mshift = shift || 100;
  my $base   = 1;
  my $grator = sub {
    return ($base += (floor(rand($mshift))||1));
  };
  return $grator;
}

unless($out eq "-") {
  open SQL, ">$out";
}
else {
  *SQL=\*STDOUT
}

# create generator functions and do some per-type initialization
map {
  my $c = $_;
  my $t = $columns{$c};
  if($t =~ /linear_timestamp(?:\((\d*)\))?/) {
    $gen{$c} = generate_linear_timestamp($1);
  }
  if($t =~ /linear_integer(?:\((\d*)\))?/) {
    $gen{$c} = generate_linear_integer($1);
  }
  if($t =~ /words/) {
    open my $dict_fh, "</usr/share/dict/words" or die($!);
    chomp(@dict = <$dict_fh>);
    close $dict_fh;
  }
} keys %columns;

if($generate_table and !$for_infile) {
  print SQL "USE $database;\n";
  print SQL "DROP TABLE IF EXISTS $table;\n\n";
  my $sql_columns = "";
  map {
    my $c = $_;
    my $t = $columns{$c};
    $t = "INTEGER PRIMARY KEY AUTO_INCREMENT" if($t eq 'int_pk');
    if($t =~ /linear_timestamp(?:\((\d*)\))?/) {
      $t = 'TIMESTAMP';
    }
    $sql_columns .= "$c $t, ";
  } sort keys %columns;
  $sql_columns =~ s/, $//;
  print SQL "CREATE TABLE IF NOT EXISTS $table ($sql_columns) ENGINE='$table_engine';\n\n";
}

my $cols_str = join(",", sort keys %columns);
my $insert_opts = ($insert_ignore ? "IGNORE" : "");
foreach my $i ($pk_start..$n_rows) {
  my $vals = "";
  map {
    my $k = $_;
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
    elsif($t =~ /linear_timestamp(?:\((\d*)\))?/) {
      if($for_infile) {
        $vals .= $gen{$k}->() .",";
      }
      else {
        $vals .= "'". $gen{$k}->() ."',";
      }
    }
    elsif($t =~ /linear_integer(?:\((\d*)\))?/) {
      if($for_infile) {
        $vals .= $gen{$k}->() .",";
      }
      else {
        $vals .= "'". $gen{$k}->() ."',";
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
    elsif($t =~ /^words\((\d+)\)/) {
      if($for_infile) {
        $vals .= generate_words($1) . ",";
      }
      else {
        $vals .= "'". generate_words($1) . "',";
      }
    }
  } sort keys %columns;
  $vals =~ s/,$//;
  if(!$for_infile) {
    print SQL "INSERT $insert_opts INTO $table ($cols_str) VALUES ($vals);\n";
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
                          Supported types are (plus see 'Psuedo Column Types' below):
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
    --primary-key-start,-P Where the primary key should begin for this gen. Default: 1

Pseudo Column Types:
In addition to the support normal column types, there are some Pseudo-types
that have specific alternate behavior other than 'random':

  int_pk:
    This type is used as the primary key for the table.
    It can only be specified once, and it produces no random values.
    The values it produces are in the range: (-P .. -r)
    That is the value of option -P (normally 1) to the value of -r (normally 500k)

  linear_timestamp(max_shift):
    This produces a timestamp column much like the regular timestamp column type,
    however, it guarantees that values inserted will start at --seed-time
    (normally NOW()), and monotonically increase in random intervals.
    The normal timestamp type will also insert values less than the current time.
    This type is best used for when simulating various types of logging tables,
    where the insert pattern has timestamps increasing with the primary key
    (if one exists).
    If max_shift specified, it indicates how much the timestamp is allowed
    to increase, in seconds.
    It defaults to 300 seconds.

  linear_int(max_shift):
    This produces an integer column that increases monotonically in random
    amounts. Normally max_shift is 100.

  words(count):
    This type is very similar to the varchar() type above, but, instead
    reads /usr/share/dict/words to generate a string composed of a
    random number of words.
    Some words are very long so, some of the strings may be truncated
    on insert, if the column is not long enough.
