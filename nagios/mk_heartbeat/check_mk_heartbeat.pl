#!/usr/bin/env perl
use strict;
use warnings FATAL => 'all';
use IPC::Open3;
use Getopt::Long;

use constant OK => 0;
use constant WARNING => 1;
use constant CRITICAL => 2;
use constant UNKNOWN => 3;

use constant MK_HEARTBEAT_DEFAULT => "/usr/bin/mk-heartbeat";
use constant WARN_MIN_LAG_DEFAULT => 0;
use constant CRIT_MIN_LAG_DEFAULT => 0;
use constant WARN_MAX_LAG_DEFAULT => 2;
use constant CRIT_MAX_LAG_DEFAULT => 2;

use constant DB_USER_DEFAULT => 'heartbeat';
use constant DB_PASS_DEFAULT => 'hb2';
use constant DB_SCHEMA_DEFAULT => 'heartbeat';
use constant DB_TABLE_DEFAULT => 'heartbeat';

# Replication can be off by negative times if the clocks are not completely synced.
# Default is 0 to go critical immediately if this situation is encountered.
my $warn_min = WARN_MIN_LAG_DEFAULT;
my $crit_min = CRIT_MIN_LAG_DEFAULT;
my $warn_max = WARN_MAX_LAG_DEFAULT;
my $crit_max = CRIT_MAX_LAG_DEFAULT;

my $mk_heartbeat_path = MK_HEARTBEAT_DEFAULT;

my $db_host = undef;
my $db_user = DB_USER_DEFAULT;
my $db_pass = DB_PASS_DEFAULT;
my $db_schema = DB_SCHEMA_DEFAULT;
my $db_table = DB_TABLE_DEFAULT;

sub usage {
  print STDERR "".
  "Usage:\n".
  "  perl check_mk_archiver.pl [-h]\n".
  "\n".
  "All options should be considered required unless the defaults suit your needs.\n".
  "Options:\n".
  "  --warn-min=i           Warn minimum lag. Negative allowed. Default: ". WARN_MIN_LAG_DEFAULT . "\n".
  "  --crit-min=i           Critial minimum lag. Negative allowed. Default: ". CRIT_MIN_LAG_DEFAULT . "\n".
  "  --warn-max=i           Warn maximum lag. Default: ". WARN_MAX_LAG_DEFAULT . "\n".
  "  --crit-max=i           Critial maximum lag. Default: ". CRIT_MAX_LAG_DEFAULT . "\n".
  "  --db-host=s            (Mandatory) Database server to connect to. No Default.\n".
  "  --db-user=s            (Mandatory) Database user to use. Default: ". DB_USER_DEFAULT ."\n".
  "  --db-pass=s            (Mandatory) Database pass to use. Default: ". DB_PASS_DEFAULT ."\n".
  "  --db-schema=s          (Mandatory) Database schema to use. Default: ". DB_SCHEMA_DEFAULT ."\n".
  "  --db-table=s           (Mandatory) Database table to use. Default: ". DB_TABLE_DEFAULT ."\n".
  "  --mk-heartbeat-path=s  Path to mk-heartbeat. Default: ". MK_HEARTBEAT_DEFAULT . "\n";
}

GetOptions(
  'help' => sub { usage(); exit(1); },
  'warn-min=i' => \$warn_min,
  'crit-min=i' => \$crit_min,
  'warn-max=i' => \$warn_max,
  'crit-max=i' => \$crit_max,
  'db-host=s' => \$db_host,
  'db-user=s' => \$db_user,
  'db-pass=s' => \$db_pass,
  'db-schema=s' => \$db_schema,
  'db-table=s'  => \$db_table,
  'mk-heartbeat-path=s' => \$mk_heartbeat_path
);

unless($db_host or $db_pass or $db_user or $db_schema or $db_table) {
  usage();
  exit(UNKNOWN);
}

my $rint = undef;
my $rstr = undef;

eval {
  my ($in_fh, $out_fh);
  my $pid = open3($in_fh, $out_fh, undef,
    'perl', $mk_heartbeat_path,
    '--host', $db_host,
    '--user', $db_user,
    '--password', $db_pass,
    '--database', $db_schema,
    '--table', $db_table,
    '--check'
  );
  close($in_fh);
  die("Unable to spawn $mk_heartbeat_path") unless($pid);

  chomp($rstr = <$out_fh>);
  close($out_fh);
  waitpid($pid, 0);

  if($rstr =~ /^(-?\d+)s?$/) {
    $rint = $1;
  }
  else {
    die($rstr ."\n");
  }
};
if($@) {
  print("UNKNOWN: Caught exception: $@");
  exit(UNKNOWN);
}

if(not defined($rint)) {
  print "CRITICAL: mk-heartbeat returned: $rstr";
  exit(CRITICAL);
}
elsif($rint < $crit_min) {
  print "CRITICAL: Lag ($rint) less than $crit_min seconds.";
  exit(CRITICAL);
}
elsif($rint < $warn_min) {
  print "WARNING: Lag ($rint) less than $warn_min seconds.";
  exit(WARNING);
}
elsif($rint > $crit_max) {
  print "CRITICAL: Lag ($rint) greater than $crit_max seconds.";
  exit(CRITICAL);
}
elsif($rint > $warn_max) {
  print "WARNING: Lag ($rint) greater than $warn_max seconds.";
  exit(WARNING);
}
else {
  print "OK: Lag $rint greater than (crit:$crit_min warn:$warn_min) and $rint less than (crit:$crit_max warn:$warn_max).";
  exit(OK);
}
