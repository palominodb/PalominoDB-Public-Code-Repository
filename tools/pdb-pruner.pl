#!/usr/bin/env perl
use strict;
use warnings;
use DBI;
use Data::Dumper;
use Getopt::Long;
use Pod::Usage;
use DateTime::Format::Strptime;

use constant NO_SELECTION => -1;
use constant USE_INFORMATION_SCHEMA => 1;
use constant USE_TABLE_NAME => 2;

my $db_host = undef;
my $db_schema = undef;
my $db_user = undef;
my $db_pass = undef;
my $table_prefix = undef;
my $time_format = undef;
my $table_age = undef;
my $drop_sleep = undef;

my $selection_method=NO_SELECTION;

my $pretend = 0;
my $debug = 0;

GetOptions(
  'help' => sub { pod2usage(1); },
  'h|db-host=s' => \$db_host,
  'u|db-user=s' => \$db_user,
  'p|db-pass=s' => \$db_pass,
  'd|db-schema=s' => \$db_schema,
  'tp|table-prefix=s' => \$table_prefix,
  'tf|time-format=s' => sub { $time_format = DateTime::Format::Strptime->new(pattern => $_[1], time_zone => "local") },
  'a|age=s' => \$table_age,
  's|sleep=s' => \$drop_sleep,
  'pretend' => \$pretend,
  'debug' => \$debug,
  'by-create' => sub { $selection_method=USE_TABLE_NAME; },
  'by-is' => sub { $selection_method=USE_INFORMATION_SCHEMA; }
);

if(not defined $db_host or not defined $db_user or not defined $db_pass or not defined $db_schema or not defined $table_prefix or not defined $table_age) {
  pod2usage(-message => "Error: --db-host, --db-user, --db-pass, --db-schema, --table-prefix, and --age are required.", -verbose => 1);
}

if($selection_method==NO_SELECTION) {
  pod2usage(-message => "Error: One of: --by-is or --by-create are required.", -verbose => 1);
}

if($selection_method==USE_TABLE_NAME and not defined $time_format) {
  pod2usage(-message => "Error: If using --by-create you must use --time-format.", -verbose => 1);
}

if($selection_method==USE_TABLE_NAME and $table_prefix !~ /^[A-Za-z0-9_\$]+$/) {
  pod2usage(-message => "Error: When using --by-create, --table-prefix must only contain allowable characters for mysql table names.", -verbose => 1);
}
elsif($selection_method==USE_TABLE_NAME and $table_prefix =~ /^[A-Za-z0-9_\$]+$/) {
  $time_format->pattern("$table_prefix". $time_format->pattern());
  print "format: ". $time_format->pattern ."\n";
}

if($selection_method==USE_INFORMATION_SCHEMA and defined $time_format) {
  print "Warning: --time-format is NOT used when using information_schema create time.\n";
}

$table_age=timestr2seconds($table_age);
dm("Dropping tables older than $table_age seconds.");
if($drop_sleep) {
  $drop_sleep=timestr2seconds($drop_sleep);
  dm("Sleeping for $drop_sleep seconds in between drops.");
}


my $dbh = DBI->connect("DBI:mysql:host=$db_host;database=$db_schema", $db_user, $db_pass);

my @tables = @{$dbh->selectcol_arrayref("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA='$db_schema' ORDER BY CREATE_TIME")};

$table_prefix =~ s/\$/\\\$/ if($selection_method==USE_TABLE_NAME);

@tables = grep(/^$table_prefix/, @tables);


foreach my $tbl (@tables) {
  my $age = -1;
  if($selection_method==USE_INFORMATION_SCHEMA) {
    $age = @{$dbh->selectrow_arrayref("SELECT UNIX_TIMESTAMP(CREATE_TIME) FROM information_schema.tables WHERE TABLE_SCHEMA='$db_schema' AND TABLE_NAME='$tbl'")}[0];
  }
  elsif($selection_method==USE_TABLE_NAME) {
    $age =  $time_format->parse_datetime($tbl)->epoch;
  }

  if((time-$age) > $table_age) {
    if($pretend) {
      print("pretend: Would drop: '$tbl' created at: ", scalar(localtime($age)) ,"\n");
      next;
    }

    print "Dropping: '$tbl' created at: ". scalar(localtime($age)) ."\n";
    $dbh->do("DROP TABLE \`$tbl\`");

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

pdb-pruner.pl - v0.3 - Prune matching tables older than some age.

=head1 SYNOPSIS

pdb-pruner.pl [--help] -h host -d schema -u user -p pass -tf regex {--by-create|--by-is}

To view the complete manual run: C<perldoc /path/to/pdb-pruner.pl>

=head1 OPTIONS

=over 24

=item B<--help>

Help text.

=item B<-h,--db-host=s>

Database server.

=item B<-d,--db-schema=s>

Database schema.

=item B<-u,--db-user=u>

Database user.

=item B<-p,--db-pass=p>

Database password.

=item B<-tf,--table-format=fmt>

Time format for when --by-create is used.

It accepts the same format as C<strptime(3)> and C<strftime(3)>.
You should be careful, as some of the compound types can
match characters which are not legal for table names.

Examples:

  %Y%m%d matches 20090317
  %F     matches 2009-03-17
  %D     matches 03/17/2009 (illegal, but no checking is done).
  %d-%H  matches 17-11 (day-hour)

=item B<-tp,--table-prefix=tbl.*>

Regex to match tables against.

You can be as perverse as you like with this, since it's passed to perl's regex engine.

When using --by-create, you must not use any perl regex characters as those are not valid
characters for table names. The script will error out if you make an attempt.

=item B<-a,--age=a>

Table age.

Takes either a number or a string.
The string should be in one of the following forms: X[hmdw] or X.X[hmdw]
Where 'h' stands for 'hours', 'm' stands for 'minutes', 'd' stands for 'days', and 'w' stands for 'weeks'.

Examples: 4h, 3d, 6.5h, 1w

=item B<--by-create>

Prune by table name (parse for dates).

=item B<--by-is>

Prune by querying information_schema for create time (Default).

=item B<--debug>

Enable noise.

=item B<--pretend>

Don't act, only talk.

=back

=head1 NOTES

Presently, this script only does time comparisons in the current timezone.
Support for alternate timezones may be a future feature.

When doing date comparisons on very far back dates (farther than 200 years)
the date calculations may not return the results you expect.
This is due to drift, dst calculations, unix timestamp deficiencies, and other factors.
If you need to do comparisons on such dates, it's recommened that you run
the script with --pretend and make sure it's pruning where you like.
Then, readup on the C<DateTime> module if it is not.

