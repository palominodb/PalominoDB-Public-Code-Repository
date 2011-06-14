#!/usr/bin/env perl
# Copyright (c) 2009-2011, PalominoDB, Inc.
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
# ###########################################################################
# ProcessLog package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessLog package
# ###########################################################################

# ###########################################################################
# TableAge package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End TableAge package
# ###########################################################################

# ###########################################################################
# TableDumper package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End TableDumper package
# ###########################################################################

# ###########################################################################
# RowDumper package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End RowDumper package
# ###########################################################################

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
use Data::Dumper;


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
my $ssh_user = undef;
my $ssh_pass = undef;
my @ssh_ids  = undef;

my $date_format = "%Y%m%d"; # YYYYMMdd
my $limit = undef;

my $output_tmpl = "TABLENAME_%Y%m%d%H%M%S";
my $mode = 'table';

my $mysqldump_path = '/usr/bin/mysqldump';
my $mk_archiver_path = '/usr/bin/mk-archiver';
my %mkopts;

my $no_compress = 0;
my $pretend = 0;

sub main {
  my @ARGV = @_;
  my @mkos;
  GetOptions(
    'help' => sub { pod2usage(); },
    'git-version' => sub { print "$0 - SCRIPT_GIT_VERSION\n"; exit(0); },
    'pretend' => \$pretend,
    'email=s' => \$email,
    'mode=s'  => \$mode,
    'logfile=s' => \$logfile,
    'db-host=s'  => \$db_host,
    'db-user=s'  => \$db_user,
    'db-pass=s'  => \$db_pass,
    'db-schema=s'  => \$db_schema,
    'table=s' => \$table,
    'date-format=s' => \$date_format,
    'ssh-host=s' => \$ssh_host,
    'ssh-user=s' => \$ssh_user,
    'ssh-pass=s' => \$ssh_pass,
    'ssh-id=s'   => \@ssh_ids,
    'table-prefix=s' => \$table_prefix,
    'column=s' => \$table_column,
    'condition=s' => \$row_condition,
    'values=s' => \$row_cond_values,
    'limit=i' => \$row_limit,
    'max-age=s' => \$limit,
    'sleep=i' => \$op_sleep,
    'output=s' => \$output_tmpl,
    'mysqldump-path=s' => \$mysqldump_path,
    'mk-archiver-path=s' => \$mk_archiver_path,
    'no-compress' => \$no_compress,
    'mkopt=s' => \@mkos
  );
  $mode = lc($mode);

  foreach(@mkos) {
    my ($k,$v) = split /=/, $_, 2;
    $mkopts{$k} = $v;
  }

  if(not $db_host) {
    pod2usage("--db-host is required.");
  }

  if($mode eq "table" and not $table and not $table_prefix) {
    pod2usage("Either --table-prefix or --table is required");
  }
  elsif($mode eq "table" and (not $table and (not $table_prefix or not $date_format or not $limit))) {
    pod2usage("--table-prefix, --date-format, and --max-age, or --table are required for table mode.");
  }
  elsif($mode eq "row" and (not $table or not $table_column or not $row_condition or not $row_limit)) {
    pod2usage("--table, --column, --condition, and --limit are required for row mode.");
  }

  $pl = ProcessLog->new($0, $logfile, $email);
  $dbh = DBI->connect("DBI:mysql:host=$db_host;database=$db_schema", $db_user, $db_pass);
  $pl->start();
  my $r=0;
  if($mode eq "table" and $table_prefix) {
    $limit = parse_limit_time($limit);
    my @tables = map { $_->[0] } @{$dbh->selectall_arrayref("SHOW TABLES FROM `$db_schema`")};
    $pl->d("Selected tables:", Dumper(\@tables));
    @tables = grep /^$table_prefix/, @tables;
    $pl->d("After grep:", Dumper(\@tables));
    my $d = TableDumper->new($dbh, $pl, $db_user, $db_host, $db_pass);
    $d->mysqldump_path($mysqldump_path);
    $tblage = TableAge->new($dbh, $pl, "${table_prefix}${date_format}");
    foreach my $t (@tables) {
      $pl->d("testing: $t");
      my $a = $tblage->age_by_name($t) || 0;
      if($a and DateTime::Duration->compare(DateTime->now(time_zone => 'local') - $a,
          DateTime::Duration->new(%$limit), $a) == 1) {
        $r=table_archive($t);
        last if($r);
        sleep $op_sleep;
      }
      else {
        $pl->d("skipped: $t");
      }
    }
  }
  elsif($mode eq "table" and $table) {
    $r=table_archive($table);
  }
  elsif($mode eq "row") {
    $r=row_archive();
  }
  else {
    $pl->e("Unknown options combination chosen. Aborting.");
  }
  $pl->end();
  if($r) {
    $pl->failure_email();
  }
  $r;
}

sub table_archive {
  my $t = shift;
  my $r=0;
  $pl->m("Starting archive of $db_schema.$t");
  my $d = TableDumper->new($dbh, $pl, $db_user, $db_host, $db_pass);
  $d->mysqldump_path($mysqldump_path);
  $d->noop($pretend);
  my $interpolated_out = out_fmt($t);
  eval {
    if($ssh_host) {
      $d->remote_dump_and_drop($ssh_user, $ssh_host, \@ssh_ids, $ssh_pass, out_fmt($t), $db_schema, $t);
      unless($no_compress) {
        $d->remote_compress($interpolated_out);
      }
    }
    else {
      $d->dump_and_drop($interpolated_out, $db_schema, $t);
      unless($no_compress) {
        $d->compress($interpolated_out);
      }
    }
  };
  if($@) {
    chomp($@);
    $pl->e($@);
    $r = 1;
  }
  $pl->m("Finished archive of $db_schema.$t");
  $r;
}

sub row_archive {
  my $r = RowDumper->new($dbh, $pl, $db_host, $db_user, $db_pass, $db_schema, $table, $table_column);
  my $rv=0;
  my $interpolated_out = out_fmt($table);
  $r->noop($pretend);
  $r->mk_archiver_path($mk_archiver_path);

  foreach(keys %mkopts) {
    $pl->d("Setting mk-archiver opt: $_=$mkopts{$_}");
    $r->mk_archiver_opt($_, $mkopts{$_});
  }

  $pl->m("Starting row dump of $db_schema.$table");
  eval {
    if($ssh_user) {
      $r->remote_archive($ssh_host, $ssh_user, @ssh_ids, $ssh_pass, $interpolated_out, $row_condition, $row_limit);
    }
    else {
      $r->archive($interpolated_out, $row_condition, $row_limit);
    }
    $pl->m("Finished row dump of $db_schema.$table.");
    unless($no_compress) {
      $pl->m("Compressing row dump of $db_schema.$table.");
      if($ssh_user) {
        $r->remote_compress($ssh_host, $ssh_user, @ssh_ids, $ssh_pass, $interpolated_out);
      }
      else {
        $r->compress($interpolated_out);
      }
      $pl->m("Finished compressing row dump of $db_schema.$table.");
    }
  };
  if($@) {
    $rv=1;
  }
  $rv;
}

sub parse_limit_time {
  my $f = shift;
  my $nf = undef;
  $pl->d("parse_limit_time: $f");
  if($f =~ /(\d+)d/i) {
    $nf = { days => int($1) };
  }
  elsif($f =~ /(\d+)w/i) {
    $nf = { weeks => int($1) };
  }
  elsif($f =~ /(\d+)m/i) {
    $nf = { months => int($1) };
  }
  elsif($f =~ /(\d+)y/i) {
    $nf = { years => int($1) };
  }
  $nf;
}

sub out_fmt {
  my $tbl = shift;
  my $t = $output_tmpl;
  $t =~ s/TABLENAME/$tbl/g;
  my $dt = DateTime->now(time_zone => 'local');
  $dt->strftime($t);
}

exit(main(@ARGV));
1;

__END__

=head1 NAME

pdb-archiver - mysqldump/rowdump and compress tables.

=head1 RISKS AND BUGS

All software has bugs. This software is no exception. Care has been taken to ensure the safety of your data, however, no guarantees can be made. You are strongly advised to test run on a staging area before using this in production.

At the time of this writing, this software could use substantially more argument error checking. The program SHOULD simply die if incorrect arguments are passed, but, it could also delete unintended things. You have been warned.

=head1 SYNOPSIS

pdb-archiver [-h]

Run with -h or --help for options. Use: C<perldoc pdb-archiver.pl> for full documentation. Set the environment variable C<Pdb_DEBUG> to something greater than 0 to see plenty of debugging information.

=head1 OPTIONS

=over 8

=item --help

This help.

Help is most awesome. It's like soap for your brain.

=item --pretend

Just report on actions that would be taken.

=item --git-version

Return the git version of this program.

This is useful for debugging and upgrading.

=item --email=s

Where to send email in the event of failure.

By default, this program sends no email.

=item --mode=s B<Mandatory.>

One of 'table', or 'row'.

'table' does table backups, and 'row' does row backups. Simple.

=item --output=s B<Mandatory.>

Path to output file.

The string may include any of the C<strftime(3)> format specifiers (see C<--date-format>).
In addition, it may also include any number of of the string C<TABLENAME> which will be replaced with the name of the table being processed.

Default: TABLENAME_%Y%m%d%H%M%S

=item --mysqldump-path=s

Default: /usr/bin/mysqldump

Path to the mysqldump binary. If dumping remotely, this is treated as the path to it on the remote machine.

=item --mk-archiver-path=s

Default: /usr/bin/mk-archiver

Path to the mk-archiver binary. If dumping remotely, this is treated as the path to it on the remote machine.

=item --logfile=s

Path for logfile. Default: ./pdb-archiver.log

=item --db-host=s B<Mandatory.>

Database hostname.

=item --db-user=s

Database user. Default: admin.

The host you are connecting from must have access.

=item --db-pass=s

Database password. Default: <empty>.

=item --db-schema=s B<Mandatory.>

Database schema to dump or work on.

=item --table=s

Table to operate on. Required for row archive mode.
In table archive mode, tool simple archives and drops this table instead of the date-processing behavior.

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

=item --max-age=s

Format: C<(\d+)[mdwyMDWY]>

That is: A number followed by one of m,d,w,y(case-insensitive).
'm' stands for month, 'd', stands for day, 'y' is year, 'w' is 'week'.

Only for use in table mode.

Tables older than max-age will be mysqldump'ed, compressed, and dropped.

=item --limit=i

How many rows to backup+delete at a time. Default: 5_000.

=back


