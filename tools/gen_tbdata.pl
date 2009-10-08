#!/opt/local/bin/perl
use strict;
use warnings;

use Getopt::Long;
use Pod::Usage;
use Data::Dumper;

my $out="random_test_data.sql";
my $table="test_data";
my $database="test";
my $n_rows = 500_000;

my $generate_table=0;
my $table_engine="InnoDB";
my %columns = ( "name" => "char(255)", "value" => "char(255)", "last_updated" => "timestamp" );

GetOptions(
  "h|help" => sub { pod2usage(); },
  "t|table=s" => \$table,
  "d|database=s" => \$database,
  "o|out=s" => \$out,
  "g|generate-table" => \$generate_table,
  "c|column=s" => \%columns,
  "r|rows=i" => \$n_rows,
  "e|engine=s" => \$table_engine
);

delete $columns{'id'}; # Hardcoded as existing.

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
  return( int(time - rand(2**32)) );
}

#my %generators = {
#  "integer" => \&generate_integer,
#  "varchar" => \&generate_varchar,
#  "timestamp" => \&generate_timestamp
#};


unless($out eq "-") {
  open SQL, ">$out";
}
else {
  *SQL=\*STDOUT
}

if($generate_table) {
  print SQL "USE $database;";
  print SQL "DROP TABLE IF EXISTS $table;\n\n";
  my $sql_columns = "";
  map {
    my $c = $_;
    my $t = $columns{$c};
    $sql_columns .= ", $c $t";
  } sort keys %columns;
  print SQL "CREATE TABLE IF NOT EXISTS $table (id INTEGER PRIMARY KEY AUTO_INCREMENT$sql_columns) ENGINE='$table_engine';\n\n";
}

my $cols_str = join(",", sort keys %columns);
foreach (0..$n_rows) {
  my $vals = "";
  map {
    my $t = $columns{$_};
    if($t =~ /^integer\((\d+)\)/) {
      $vals .= generate_integer($1) . ",";
    }
    elsif($t =~ /^integer/) {
      $vals .= generate_integer(10) . ",";
    }
    elsif($t =~ /^timestamp/) {
      $vals .= generate_timestamp() . ",";
    }
    elsif($t =~ /^(?:var)?char\((\d+)\)/) {
      $vals .= "'". generate_varchar($1) . "',";
    }
  } sort keys %columns;
  $vals =~ s/,$//;
  print SQL "INSERT INTO $table ($cols_str) VALUES ($vals);\n";
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
