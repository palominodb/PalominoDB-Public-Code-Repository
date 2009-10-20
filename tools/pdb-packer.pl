#!/usr/bin/perl
use strict;
use warnings;

use DBI;
use Getopt::Long;
use Pod::Usage;
use Mail::Send;
use Sys::Hostname;
use Digest::SHA1;
use Time::HiRes;
use Net::SSH::Perl;
use DateTime;

use constant DEFAULT_LOG => "pdb-packer.log";
use constant DEFAULT_DATADIR => "/var/lib/mysql";
use constant DEFAULT_WEEKLY_FORMAT => "%V%g";

my $pretend = 0;
my $logfile = DEFAULT_LOG;
my $email_to = undef;

my $db_master = undef;
my $db_slave = undef;
my $db_schema = undef;
my $db_user   = undef;
my $db_pass = undef;

my $table = undef;
my $db_datadir = DEFAULT_DATADIR;
my $rotate_format =  DEFAULT_WEEKLY_FORMAT;

my $db_convert = 0;

my $ssh_user = "root";
my $ssh_id   = undef;

my $debug = 0;

my $script = "$0 " . join(' ', @ARGV);
$script =~ s/db-pass=\S+/db-pass=********/g;
$0 = $script;

GetOptions(
  "help" => sub {
    pod2usage();
  },
  "d|debug" => \$debug,
  "pretend|p" => \$pretend,
  "db-master=s" => \$db_master,
  "db-slave=s" => \$db_slave,
  "db-schema=s" => \$db_schema,
  "db-user=s" => \$db_user,
  "db-pass=s" => \$db_pass,
  "logfile=s" => \$logfile,
  "datadir=s" => \$db_datadir,
  "table=s" => \$table,
  "rotate-format" => \$rotate_format,
  "email-to=s" => \$email_to,
  "ssh-user" => \$ssh_user,
  "ssh-id" => \$ssh_id,
  "convert" => \$db_convert
);

if(!$db_master or !$db_schema or !$db_user or !$db_pass) {
  pod2usage("Options --db-master, --db-schema, --db-user, and --db-pass are REQUIRED.");
}

if(!$table) {
  pod2usage("--table is a required option.");
}

# Generate something that's hopefully unique-ish
my $run_id = Digest::SHA1::sha1_hex(time . rand() . $0);
my $dt = DateTime->now;
my $rotate_serial = $dt->strftime($rotate_format);

if($pretend) {
  my $dbh = DBI->connect("DBI:mysql:database=$db_schema;host=$db_master", $db_user, $db_pass) or die("Unable to connect to $db_master. DBI sayz: $!");
  my $engine = $dbh->selectrow_arrayref("SELECT ENGINE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='${table}' AND TABLE_SCHEMA='${db_schema}'");
  $engine = $engine->[0];
  print("RUN ID: $run_id\n");
  print("Create Replacement table SQL: CREATE TABLE ${table}_rplc LIKE $table\n");
  print("Swap tables SQL: RENAME TABLE ${table} TO ${table}_$rotate_serial, ${table}_rplc TO $table\n");
  if($db_convert) {
    print("Alter engine: ALTER TABLE ${table}_$rotate_serial ENGINE=MyISAM\n");
  }
  print("Run myisampack\@$db_master: /usr/bin/myisampack $db_datadir/${table}_$rotate_serial\n");
  print("Run myisamchk\@$db_master: /usr/bin/myisamchk -dvv ${table}_$rotate_serial\n");
  if($db_slave) {
    print("Run myisampack\@$db_slave: /usr/bin/myisampack $db_datadir/${db_schema}/${table}_$rotate_serial\n");
    print("Run myisamchk\@$db_slave: /usr/bin/myisamchk -dvv $db_datadir/${db_schema}/${table}_$rotate_serial\n");
  }
  exit(0);
}

open LOG, ">>$logfile" or email_and_die("Unable to open logfile: $logfile");

my $starttime = time;
msg("Starting; RUN ID: $run_id");
my $dbh = DBI->connect("DBI:mysql:database=$db_schema;host=$db_master", $db_user, $db_pass) or email_and_die("Unable to connect to $db_master.");
msg("Connected to $db_master");
my $engine = $dbh->selectrow_arrayref("SELECT ENGINE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='${table}' AND TABLE_SCHEMA='${db_schema}'");
$engine = $engine->[0];
$dbh->do("CREATE TABLE ${table}_rplc LIKE $table") or email_and_die("Unable to create new table (${table}_rplc). ". DBI->errstr);
msg("Created new table ${table}_rplc.");
$dbh->do("RENAME TABLE ${table} TO ${table}_$rotate_serial, ${table}_rplc TO $table") or email_and_die("Unable to swap tables. ". DBI->errstr);
msg("Renamed table ${table} to ${table}_$rotate_serial, ${table}_rplc to ${table}.");
if($db_convert and lc($engine) eq "innodb") {
  $dbh->do("ALTER TABLE ${table}_$rotate_serial ENGINE=MyISAM") or email_and_die("Unable to change table to MyISAM. ". DBI->errstr);
  msg("Converted to MyISAM for packing.");
}

msg("Packing on master.");
dbpack($db_master, $ssh_user, $ssh_id);
msg("Packing on slave.");
dbpack($db_slave, $ssh_user, $ssh_id);

exit(0);

sub dbpack {
  my $host = shift;
  my $user = shift;
  my $id = shift;
  msg("dbpack: SSHing to $host");
  my $ssh = Net::SSH::Perl->new($host, { 'identity_files' => $id, 'debug' => $debug });
  $ssh->login($user);
  msg("dbpack: Logged into $host as $user\n");
  my ($stdout, $stderr, $exit) = $ssh->cmd("/usr/bin/myisampack $db_datadir/${db_schema}/${table}_$rotate_serial");
  $stdout = "" if(not defined $stdout);
  $stderr = "" if(not defined $stderr);
  if($exit != 0) {
    email_and_die("myisampack on $host failed!\nSTDOUT:\n$stdout\nSTDERR:\n$stderr");
  }
  else {
    msg("Packing successful.");
  }
  ($stdout, $stderr, $exit) = $ssh->cmd("/usr/bin/myisamchk -rq $db_datadir/${db_schema}/${table}_$rotate_serial");
  $stdout = "" if(not defined $stdout);
  $stderr = "" if(not defined $stderr);
  if($exit != 0) {
    email_and_die("myisamchk on $host failed!\nSTDOUT:\n$stdout\nSTDERR:\n$stderr");
  }
  else {
    msg("Check successful.");
  }
}

sub msg {
  my $m = shift;
  print LOG "". scalar localtime() . ": $m\n";
  if($debug) {
    print scalar localtime() . ": $m\n";
  }
}

sub email_and_die {
  my $extra = shift;
  die("Not emailing: $extra") if(not defined $email_to);
  msg("Emailing out failure w/ extra: $extra\n");
  my $msg = Mail::Send->new(Subject => "pdb-packer.pl FAILED", To => $email_to);
  my $fh = $msg->open;
  print $fh "pdb-packer.pl on ". hostname() . " failed at ". scalar localtime() ."\n";
  print $fh "\nThe Error: $extra\n";
  print $fh "RUN ID: $run_id";
  $fh->close;
  die($extra)
}

__END__

=head1 NAME

pdb-packer.pl - Rotate and Compress tables.

=head1 SYNOPSIS

pdb-packer.pl [-h] [-p] [--email-to=addr] --db-master=host --db-user=user --db-schema=database --db-pass=password --table=tbl

=head1 OPTIONS

=over 4

=item B<--debug>

Be noisy on the console.

=item B<--pretend>

Don't actually do anything. Just report what would happen.

=item B<--db-master=host>

Hostname of the master db server. Used for mysql and ssh connections.

=item B<--db-slave=host>

Hostname of the slave db server. Used for mysql and ssh connections.
Need not be specified if there is no slave.

=item B<--db-schema=database>

Database to use.

=item B<--table=name>

Table to rotate and pack.

=item B<--db-user=user>

Username for mysql connection.

=item B<--db-pass=password>

Password for mysql connection.

=item B<--logfile=path>

Alternate path for the logfile.
Defaults to './pdb-packer.log' (without quotes).

=item B<--datadir=path>

Path to the mysql datadir on the remote db server.
Defaults to: '/var/lib/mysql'.

=item B<--rotate-format=format>

Format to append to the table name. Can use any of the formatting codes from strftime(3).
Defaults to: '%V%g' (week number 01-53, 2-digit year)
Example:
    table: testbl
    rotated name: testbl_2909

=item B<--email-to=addr>

Where to email failures. No default. If not specified, pdb-packer will not email out.

=item B<--ssh-user=user>

Username to ssh in with. Defaults to 'root'.

=item B<--ssh-id=path>

Identity file to use. Passwords are not supported.

=item B<--convert>

Will convert the rotated table to MyISAM, if needed.

=back
