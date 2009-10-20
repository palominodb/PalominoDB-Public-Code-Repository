#!/usr/bin/env perl
use strict;
use warnings;
use DBI;
use Data::Dumper;
use Getopt::Long;
use Pod::Usage;

my $db_host = undef;
my $db_schema = undef;
my $db_user = undef;
my $db_pass = undef;
my $table_format = undef;
my $table_age = undef;
my $drop_sleep = undef;
my $by_create = 0;
my $by_is = 1;

my $pretend = 0;
my $debug = 0;

GetOptions(
  'help' => sub { pod2usage(1); },
  'h|db-host=s' => \$db_host,
  'u|db-user=s' => \$db_user,
  'p|db-pass=s' => \$db_pass,
  'd|db-schema=s' => \$db_schema,
  'tf|table-format=s' => \$table_format,
  'a|age=s' => \$table_age,
  's|sleep=s' => \$drop_sleep,
  'pretend' => \$pretend,
  'debug' => \$debug,
  'by-create' => \$by_create,
  'by-is' => \$by_is
);

if(not defined $db_host or not defined $db_user or not defined $db_pass or not defined $db_schema or not defined $table_format or not defined $table_age) {
  pod2usage(-message => "Error: --db-host, --db-user, --db-pass, --db-schema, --table-format, and --age are required.", -verbose => 1);
}

if(!$by_is and !$by_create) {
  pod2usage(-message => "Error: One of: --by-is or --by-create are required.", -verbose => 1);
}

if($by_create) {
  die("Error: --by-create is not supported yet.");
}

$table_age=timestr2seconds($table_age);
dm("Dropping tables older than $table_age seconds.");
if($drop_sleep) {
  $drop_sleep=timestr2seconds($drop_sleep);
  dm("Sleeping for $drop_sleep seconds in between drops.");
}

my $dbh = DBI->connect("DBI:mysql:host=$db_host;database=$db_schema", $db_user, $db_pass);

my @tables = @{$dbh->selectcol_arrayref("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA='$db_schema' ORDER BY CREATE_TIME")};

@tables = grep(/$table_format/, @tables);

foreach my $tbl (@tables) {
  my $age = -1;
  if($by_is) {
    $age = @{$dbh->selectrow_arrayref("SELECT UNIX_TIMESTAMP(CREATE_TIME) FROM information_schema.tables WHERE TABLE_SCHEMA='$db_schema' AND TABLE_NAME='$tbl'")}[0];
  }
  else {
    # TODO: Table name age selecting code
  }

  if((time-$age) > $table_age) {
    if($pretend) {
      print("pretend: Would drop: '$tbl' created at: ", scalar(localtime($age)) ,"\n");
      next;
    }

    print "Dropping: '$tbl' created at: ". scalar(localtime($age)) ."\n";
    $dbh->do("DROP TABLE $tbl");

    sleep($drop_sleep) if($drop_sleep);
  }
  else {
    dm("Not dropping '$tbl' created at: ". scalar(localtime($age)));
  }
}

sub dm {
  print 'debug: ', @_, "\n" if($debug);
}

sub timestr2seconds {
  my $age_str=shift;
  my $age_int=0;
  my %tspecs = ( 'h' => 60*60, 'm' => 60, 'd' => 3600*24, 'w' => 3600*24*7 );
  if($age_str =~ /(\d+(?:\.\d+)?)([hmdw])/) {
    $age_int=$1*$tspecs{$2};
  }
  else {
    $age_int=$age_str; }
  $age_int
}
__END__

=head1 NAME

table-pruner.pl - v0.1 - Prune matching tables older than some age.

=head1 SYNOPSIS

table-pruner.pl [--help] -h host -d schema -u user -p pass -tf regex {--by-create|--by-is}

=head1 OPTIONS

=over 24

=item B<--help>

Help text.

=item B<-h,--db-host=s>

Database server.

=item B<-u,--db-user=u>

Database user.

=item B<-p,--db-pass=p>

Database password.

=item B<-tf,--table-format=tbl.*>

Regex to match tables against.

You can be as perverse as you like with this, since it's passed to perl's regex engine.

=item B<-a,--age=a>

Table age.

Takes either a number or a string.
The string should be in one of the following forms: X[hmdw] or X.X[hmdw]
Where 'h' stands for 'hours', 'm' stands for 'minutes', 'd' stands for 'days', and 'w' stands for 'weeks'.

Examples: 4h, 3d, 6.5h

=item B<--by-create>

Prune by table name (parse for dates). Not implemented.

=item B<--by-is>

Prune by querying information_schema for create time (Default).

=item B<--debug>

Enable noise.

=item B<--pretend>

Don't act, only talk.

=back

