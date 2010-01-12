#!/usr/bin/env perl
use strict;
use warnings;

# ###########################################################################
# ProcessLog package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessLog package
# ###########################################################################

# ###########################################################################
# TablePartitions package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End TablePartitions package
# ###########################################################################

package main;
use strict;
use warnings;
use English qw(-no_match_vars);

use ProcessLog;
use TablePartitions;

use DBI;
use Getopt::Long;
use Pod::Usage;
use DateTime;

# Defined since Nagios::Plugin doesn't always exist.
use constant OK       => 0;
use constant WARNING  => 1;
use constant CRITICAL => 2;
use constant UNKNOWN  => 2;

my (
  $db_host,
  $db_user,
  $db_pass,
#  $db_defaults,
  $db_schema,
  $db_table,
  $range,
  $verify
);

GetOptions(
  "help" => sub { pod2usage(); },
  "host|h=s" => \$db_host,
  "user|u=s" => \$db_user,
  "pass|p=s" => \$db_pass,
  "database|d=s" => \$db_schema,
  "table|t=s" => \$db_table,
  "range|r=s" => \$range,
  "verify|n=s" => \$verify
);

unless($db_host and $db_user and $db_pass and $db_schema and $db_table and $range and $verify) {
  pod2usage(-message => "All parameters are required.");
}

$range = lc($range);

unless($range =~ /^(?:days|weeks|months)$/) {
  pod2usage(-message => "Range must be one of: days, weeks, or months.");
}

my $dbh =  DBI->connect("DBI:mysql:$db_schema;host=$db_host", $db_user, $db_pass, { RaiseError => 1, PrintError => 0, AutoCommit => 0});

my $pl = ProcessLog->null;#new('', '/dev/null', undef);
my $parts = TablePartitions->new($pl, $dbh, $db_schema, $db_table);
my $last_ptime = 0;
my $last_p = $parts->last_partition;

if($last_p->{description} eq 'MAXVALUE') {
  $last_p = $parts->partitions->[-2];
}

$last_ptime = to_date($parts->desc_from_days($last_p->{name}));
my $today = DateTime->today(time_zone => 'local');

my $du = $last_ptime - $today;

if($range eq 'days') {
  $du = $last_ptime->delta_days($today);
}

$dbh->disconnect;

if($du->in_units($range) < $verify) {
  print "CRITICAL: Not enough partitions. ". $du->in_units($range) . " $range less than $verify $range\n";
  exit(CRITICAL);
}
else {
  print "OK: Enough partitions. ". $du->in_units($range) . " $range greater than, or equal to $verify $range\n";
  exit(OK);
}

print 'UNKNOWN: Very strange error. How did we get here?';
exit(UNKNOWN);

sub to_date {
  my ($dstr) = @_;
  my ($year, $month, $day) = split '-', $dstr;
  return DateTime->new(year => $year, month => $month, day => $day, time_zone => 'local')->truncate( to => 'day' );
}

=pod

=head1 NAME

table_partitions.pl - Ensure partitions exist for N days/weeks/months.

=head1 SYNOPSIS

table_partitions.pl -h <host> -d <schema> -t <table> -r <range> -n <num>

options:

  --help         This help.
  --host,-h      DB host.
  --user,-u      DB user.
  --pass,-p      DB pass.
  --database,-d  DB database(schema).
  --table,-t     DB table.
  --range,-r     One of: days, weeks, or months.
  --verify,-n    How many -r to ensure exist.

=cut

1;
