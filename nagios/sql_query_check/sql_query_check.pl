#!/usr/bin/perl
use strict;
use warnings;

use DBI;
use Getopt::Long;
use Data::Dumper;
use Pod::Usage;

use constant OK       => 0;
use constant WARNING  => 1;
use constant CRITICAL => 2;
use constant UNKNOWN  => 3;

my %o; # options hash

# Default values for user/pass
# Replace these so that user/pass need not be specified on
# the commandline.
$o{'user'} = "nagios";
$o{'pass'} = "password";
$o{'alert-on'} = "any";

GetOptions(\%o,
  "help|h",
  "host=s",
  "database|d=s",
  "user|u=s",
  "pass|p=s",
  "query|q=s",
  "check-column|cc=s",
  "warning|w=s",
  "critical|c=s",
  "additional|a=s",
  "alert-on|ao=s",

  "debug"
);

$o{'query'} = $o{'q'} if(exists $o{'q'});
$o{'check-column'} = $o{'cc'} if(exists $o{'cc'});
$o{'warning'} = $o{'w'} if(exists $o{'w'});
$o{'critical'} = $o{'c'} if(exists $o{'c'});
$o{'additional'} = $o{'a'} if(exists $o{'a'});
$o{'user'} = $o{'u'} if(exists $o{'u'});
$o{'pass'} = $o{'p'} if(exists $o{'p'});
$o{'database'} = $o{'d'} if(exists $o{'d'});
$o{'alert-on'} = $o{'ao'} if(exists $o{'ao'});

if(exists $o{'help'}) {
  pod2usage(-verbose => 2);
}
if(exists $o{'h'}) {
  pod2usage();
}

if(not exists $o{'host'}) {
  pod2usage("Must specify --host.");
}

if(not exists $o{'query'}) {
  pod2usage("Must have a query!");
}
if(not exists $o{'check-column'}) {
  pod2usage("Must have check-column.");
}

if($o{'alert-on'} !~ /any|all/) {
  pod2usage("--alert-on Must be 'any' or 'all'. Not: $o{'alert-on'}.");
}

my $dbh = DBI->connect("DBI:mysql:host=$o{'host'}", $o{'user'}, $o{'pass'}, 
  { PrintError => 1, RaiseError => 0 });

if(exists $o{'database'}) {
  $dbh->do("use $o{'database'}");
}

# Get query results as an array of hashes.
#my @results = @{$dbh->selectall_arrayref($o{'query'}, { Slice => {} })};
my @results = @{$dbh->selectall_arrayref($o{'query'})};

if($o{'debug'}) {
  use Data::Dumper;
  print Dumper(\@results);
}

# Loop over all results, and ensure they match the ranges according
# to --alert-on.
# The common case is to only select a single row, so usually
# this loop only runs once.
my $error_count=0; # start out assuming all are within tollerance.
my $warn_count=0;
my $crit_count=0;
my $result = OK;
my $result_str = $o{'query'} . " returns: ";

# The loop does:
# 1. appends the results to the result string
# 2. checks if the value is in the critical range, and sets 
#    $result = CRITICAL, if it is.
# 3. else checks if value is in warning range, and sets
#    $result = WARNING if $result is not CRITICAL
foreach my $r (@results) {
  my $v = $r->[$o{'check-column'}];
  $result_str .= "'". join(",", @$r) . "'";
  if($o{'debug'}) {
    print "crit in-range: ". in_range($v, $o{'critical'}) . "\n";
    print "warn in-range: ". in_range($v, $o{'warning'}) . "\n";
  }
  if(in_range($v, $o{'critical'}) == 1) {
    $error_count++;
    $crit_count++;
    $result = CRITICAL;
  }
  elsif(in_range($v, $o{'warning'}) == 1) {
    $error_count++;
    $warn_count++;
    $result = WARNING if($result != CRITICAL);
  }
}

if($o{'debug'}) {
  print "err-count: $error_count\n";
  print "crit-count: $crit_count\n";
  print "warn-count: $warn_count\n";
  print "result: $result\n";
}

# downgrade $result to ok, if we're checking all results and not all of them
# had an error.
# otherwise, choose the greater of the two counts.
if($o{'alert-on'} eq "all" and $error_count != scalar @results) {
  $result = OK;
}
elsif($o{'alert-on'} eq "all" and $error_count == scalar @results) {
  if($crit_count >= $warn_count) {
    $result = CRITICAL;
  }
  else {
    $result = WARNING;
  }
}

if($result == CRITICAL) {
  $result_str = "CRITICAL: $result_str";
}
elsif($result == WARNING) {
  $result_str = "WARNING: $result_str";
}
else {
  $result_str = "OK: $result_str";
}

$result_str .= $o{'additional'} if(exists $o{'additional'});
$result_str .= "\n";

print $result_str;
exit($result);

# Checks to see if the @_[0] is inside @_[1].
# Argument types must match.
# I.e., if @_[0] is 300, then the range must be numeric also.
# If @_[0] is "done", then the "range" must be a string as well.
sub in_range {
  my ($arg, $range) = @_;
  my ($bottom, $top);
  if($range =~ /:/) {
    ($bottom, $top) = split /:/, $range;
  }
  else {
    $top = $range;
    $bottom = $top;
  }
  if($o{'debug'}) {
    print "arg: $arg\nrange: $range\n";
    print "top: $top\nbottom: $bottom\n";
  }
  if($arg !~/\d+/) {
    if($arg eq $bottom or $arg eq $top) {
      return 1;
    }
  }
  else {
    if($bottom == $top and $arg >= $top) {
      return 1;
    }
    if($arg >= $bottom and $arg <= $top) {
      return 1;
    }
  }
  return 0;
}

__END__

=head1 NAME

sql_query_check.pl - Enhanced 'check_mysql_query'.

=head1 SYNOPSIS

Usage: sql_query_check.pl [--help] --query=SQL --check-column=Name|Num -w[s:e] -c[s:e]

  --help               This help.
  --host               DB host.
  --database,-d        Database to use.
  --user,-u            User to connect with.
  --pass,-p            Password to connect with.
  --query,-q           Query to execute.
  --check-column,-cc   Column which contains values to check.
  --warning,-w         Warning range.
  --critical,-c        Critical range.
  --additional,-a      Additional string to append to nagios status.
  --alert-on,-ao       any/all - Any outside range/All outside range.

=head1 OPTIONS

=over 8

=item B<--help>

This help message.

=item B<--host>

Database hostname or IP to connect to. Only port 3306 is supported.

=item B<--user>

Database user to connect with. Must have priviledges to execute the query.
The default value for this is 'nagios', and can be changed by editing the script.

=item B<--pass>

Password for the database user. The default value is 'password'. Change by editing 
the script.

=item B<--query,-q>

Query to execute.

=item B<--check-column,-cc>

The column number to to do bounds checking against. Starts from 0.

=item B<--alert-on,-ao>

How to handle alerting for multiple rows. Can be: any, or all.
When 'any' is specified, any row not conforming inside the error ranges will set an error.
When 'all' is specified, all rows must have an error, and the return value is determined by the largest between critical and warning results. Critical wins if they are equal.

=item B<--warning,-w>

The range for warning results. Ranges are specified like: start:end.
If a single number is specified then, it is treated as the end of the range.

The range can be non-numeric in which case only the word(s) specified will be considered as 'warning' value(s).
Examples:
  300:500 (numbers between 300 and 500 are considered warning)
  500 (numbers above 500 are considered warning)
  done (words matching 'done' are considered warning)
  in_progress:done_with_errors (either 'in_progress' or 'done_with_errors' are warnings)

=item B<--critical,-c>

The range for critical results. Same format as the B<--warning> ranges.

=item B<--additional,-a>

Additional text to add to the results verbatim.

=back
