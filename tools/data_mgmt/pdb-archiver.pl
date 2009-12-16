#!/usr/bin/env perl
use strict;
use warnings;

## BEGIN ProcessLog.pm GIT_VERSION ############################################
## END ProcessLog.pm ##########################################################

## BEGIN TableAge.pm GIT_VERSION ##############################################
## END TableAge.pm ############################################################

## BEGIN TableDumper.pm GIT_VERSION ###########################################
## END TableDumper.pm #########################################################

## BEGIN RowDumper.pm GIT_VERSION #############################################
## END RowDumper.pm ###########################################################

package pdb_archiver;
use strict;
use warnings;
use ProcessLog;
use TableAge;
use TableDumper;
use RowDumper;

use Getopt::Long;
use Pod::Usage;
use DateTime;


my $pl; # ProcessLog.
my $dbh;
my $tblage;

my $email = undef;
my $logfile = "$0.log";

my $db_host = undef;
my $db_user = 'admin';
my $db_pass = undef;
my $db_schema = 'test';

my $table = undef;
my $table_prefix = undef;
my $table_column = undef;
my $row_condition = "";
my $row_cond_values = "";
my $row_limit = 5_000;
my $op_sleep = 10;

my $ssh_host = undef;
my $ssh_user = 'mysql';
my $ssh_pass = undef;
my @ssh_ids  = undef;

my $date_format = "%Y%m%d"; # YYYYMMdd

my $output_tmpl = "TABLENAME_%Y%m%d%H%M%S";
my $mode = 'table';

sub main {
  my @ARGV = @_;
  GetOptions(
    'help' => sub { pod2usage(); },
    'git-version' => sub { print "$0 - SCRIPT_GIT_VERSION\n"; },
    'email=s' => \$email,
    'mode=s'  => \$mode,
    'logfile=s' => \$logfile,
    'db-host=s'  => \$db_host,
    'db-user=s'  => \$db_user,
    'db-pass=s'  => \$db_pass,
    'db-schema=s'  => \$db_schema,
    'table=s' => \$table,
    'date-format' => \$date_format,
    'ssh-host=s' => \$ssh_host,
    'ssh-user=s' => \$ssh_user,
    'ssh-pass=s' => \$ssh_pass,
    'ssh-id=s'   => \@ssh_ids,
    'table-prefix=s' => \$table_prefix,
    'column=s' => \$table_column,
    'condition=s' => \$row_condition,
    'values=s' => \$row_cond_values,
    'limit=i' => \$row_limit,
    'sleep=i' => \$op_sleep,
    'output=s' => \$output_tmpl
  );
  $mode = lc($mode);

  if(not $db_host) {
    pod2usage("--db-host is required.");
  }

  if($mode eq "table" and (not $table_prefix or not $date_format)) {
    pod2usage("--table-prefix and --date-format are required for table mode.");
  }

  if($mode eq "row" and (not $table or not $table_column or not $row_condition or not $row_limit)) {
    pod2usage("--table, --column, --condition, and --limit are required for row mode.");
  }

  $pl = ProcessLog->new($0, $logfile, $email);
  $dbh = DBI->connect("DBI:mysql:host=$db_host;database=$db_schema", $db_user, $db_pass);
  $pl->start();
  if($mode eq "table") {
    table_archive();
  }
  else {
    row_archive();
  }
  $pl->end();
}

sub table_archive {
  my $d = TableDumper->new($dbh, $pl, $db_user, $db_host, $db_pass);
  my $tables = $dbh->selectall_arrayref("SHOW TABLES FROM `$db_schema`");
  $pl->d("Selected tables:", Dumper($tables));
  $tables = grep /^$table_prefix/, @$tables;
  $pl->d("After grep", Dumper($tables));
  $tblage = TableAge->new($dbh, $pl, "${table_prefix}${date_format}");
}

sub row_archive {
  my $r = RowDumper->new($dbh, $pl, $db_schema, $table, $table_column);

  $pl->m("Starting row dump of $db_schema.$table with $op_sleep second sleeps");
  while($r->dump(out_fmt($table), $row_condition, $row_limit, split(/,/, $row_cond_values))) {
    $r->drop($row_condition, $row_limit, split(/,/, $row_cond_values));
    sleep($op_sleep);
  }
  $r->finish();
  $pl->m("Finished row dump of $db_schema.$table.");
  $pl->m("Compressing row dump of $db_schema.$table.");
  $r->compress(out_fmt($table));
  $pl->m("Finished compressing row dump of $db_schema.$table.");
}

sub out_fmt {
  my $tbl = shift;
  my $t = $output_tmpl;
  $t =~ s/TABLENAME/$tbl/g;
  my $dt = DateTime->now;
  $dt->strftime($t);
}

main(@ARGV);
1;

__END__

=head1 NAME

pdb-archiver.pl - mysqldump/rowdump and compress tables.

=head1 RISKS AND BUGS

All software has bugs. This software is no exception. Care has been taken to ensure the safety of your data, however, no guarantees can be made. You are strongly advised to test run on a staging area before using this in production.

At the time of this writing, this software could use substantially more argument error checking. The program SHOULD simply die if incorrect arguments are passed, but, it could also delete unintended things. You have been warned.

=head1 SYNOPSIS

pdb-archiver.pl [-h]

Run with -h or --help for options. Use: C<perldoc pdb-archiver.pl> for full documentation. Set the environment variable C<Pdb_DEBUG> to something greater than 0 to see plenty of debugging information.

=head1 OPTIONS

=over 8

=item --help

This help.

Help is most awesome. It's like soap for your brain.

=item --git-version

Return the git version of this program.

This is useful for debugging and upgrading.

=item --email=s

Where to send email in the event of failure.

By default, this program sends no email.

=item --mode=s

One of 'table', or 'row'.

'table' does table backups, and 'row' does row backups. Simple.

=item --output=s

Path to output file.

The string may include any of the C<strftime(3)> format specifiers (see C<--date-format>).
In addition, it may also include any number of of the string C<TABLENAME> which will be replaced with the name of the table being processed.

Default: TABLENAME_%Y%m%d%H%M%S

=item --logfile=s

Path for logfile. Default: ./pdb-archiver.pl.log

=item --db-host=s

Database hostname.

=item --db-user=s

Database user.

The host you are connecting from must have access.

=item --db-pass=s

Database password.

=item --db-schema=s

Database schema to dump or work on.

=item --table=s

Table to operate on in row archive mode.

=item --date-format=s

Format to append to the table prefix.

Can use any of the formatting codes from strftime(3).
Defaults to: '%Y%m%d' (4-digit year, month, and day)
Example:
    table prefix: testbl_
    date-matching name: testbl_%Y%m%d


=item --ssh-host=s

Remote host to ssh into before working, if any.

This option and the other ssh options are only valid when doing table dumps, for now.

=item --ssh-user=s

User for ssh. Default: 'mysql'.

=item --ssh-pass=s

Password to use for ssh. Not recommended.

This password will be plainly visible in `ps` for anyone to see.

Better to use pubkey authentication, instead.

=item --ssh-id=s

Path to a private key to use for SSH.

This option may be specified multiple times to try multiple keys.

=item --table-prefix=s

When in table mode, only operate on tables starting with
this prefix.

This option is combind verbatim with the value of  --date-format
to do date parsing. Please take care to add all appropriate underscores and the like.

=item --column=s

Column to use for archiving when in 'row' mode.

=item --condition=s

SQL WHERE-clause fragment. Used to select which rows to archive
when in row mode.

=item --sleep=i

How many seconds to sleep between operations.

Default: 10

For tables this is how many seconds between dump/drops.
And for row chewing, it's how many seconds to pause between batches.

=item --limit=i

How many rows to backup+delete at a time. Default: 5_000.

=back


