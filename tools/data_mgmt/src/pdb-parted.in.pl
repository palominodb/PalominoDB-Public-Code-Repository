#!/usr/bin/env perl
# Copyright (c) 2009-2010, PalominoDB, Inc.
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

package pdb_parted;

use strict;
use warnings;
use English qw(-no_match_vars);

use ProcessLog;
use TablePartitions;

use DBI;
use Getopt::Long;
use Pod::Usage;
use DateTime;
use DateTime::Duration;
use DateTime::Format::Strptime;

use Data::Dumper;
$Data::Dumper::Indent = 0;
$Data::Dumper::Sortkeys = 1;

my $pretend = 0;
my $uneven = 0;

my $pl = 0;

sub main {
  my @ARGV = @_;
  my (
    $dsn,
    $dbh,
    $parts,
    # options
    $logfile,
    $email_to,
    $db_host,
    $db_schema,
    $db_table,
    $db_user,
    $db_pass,
    $db_file,
    $prefix,
    $range,
    $add,
    $drop,
    $i_am_sure,
    $uneven
  );

  GetOptions(
    "help" => sub { pod2usage(); },
    "pretend" => \$pretend,
    "logfile|L=s" => \$logfile,
    "email-to|E=s" => \$email_to,
    "host|h=s" => \$db_host,
    "database|d=s" => \$db_schema,
    "table|t=s" => \$db_table,
    "user|u=s" => \$db_user,
    "password|p=s" => \$db_pass,
    "defaults-file|F=s" => \$db_file,
    "prefix|P=s", \$prefix,
    "range|r=s", \$range,
    "add=i", \$add,
    "drop=i", \$drop,
    "i-am-sure", \$i_am_sure
  );

  unless($db_schema and $db_table and $prefix and $range) {
    pod2usage(-message => "--table, --database, --prefix and --range are mandatory.");
  }

  $range=lc($range);

  unless($range eq 'months' or $range eq 'days' or $range eq 'weeks') {
    pod2usage(-message => "--range must be one of: months, days, or weeks.");
  }

  unless($prefix =~ /^[A-Za-z][A-Za-z0-9_-]*$/) {
    pod2usage(-message => "--prefix ($prefix) must not include non alpha-numeric characters.");
  }

  unless($add or $drop) {
    pod2usage(-message => "--add or --drop required.");
  }

  if($db_file and ! -f $db_file) {
    pod2usage(-message => "--defaults-file $db_file doesn't exist, or is a directory.");
  }

  if($add and $drop) {
    pod2usage(-message => "only one of --add or --drop may be specified");
  }

  $dsn = "DBI:mysql:$db_schema";
  if($db_host) {
    $dsn .= ";host=$db_host";
  }
  if($db_file) {
    $dsn .= ";mysql_read_default_file=$db_file;mysql_read_default_group=client";
  }

  $dbh = DBI->connect($dsn, $db_user, $db_pass,
    { RaiseError => 1, PrintError => 0, AutoCommit => 0, ShowErrorStatement => 1 });

  $pl = ProcessLog->new('pdb-parted', $logfile || 'pdb-parted.log', $email_to);

  $pl->start;

  $parts = TablePartitions->new($pl,$dbh, $db_schema, $db_table);

  my $r = 0;
  if($add) {
    $r = add_partitions($add, $dbh, $parts, $prefix, $range, $db_schema, $db_table, $i_am_sure);
  }
  elsif($drop) {
    $r = drop_partitions($drop, $dbh, $parts, $db_schema, $db_table, $i_am_sure);
  }

  $dbh->disconnect;
  $pl->failure_email() unless($r);
  $pl->end;
  return 0;
}

sub add_partitions {
  my ($add, $dbh, $parts, $prefix, $range, $db_schema, $db_table, $i_am_sure) = @_;

  my $ret = 1;
  my $last_p = $parts->last_partition;
  my $next_pN = undef;
  my ($dur, $reqdur) = (undef, undef);
  my $today = DateTime->today(time_zone => 'local');
  my $reorganize = uc($last_p->{description}) eq 'MAXVALUE';
  if($reorganize) {
    $last_p = $parts->partitions()->[-2];
    if($parts->has_maxvalue_data and !$i_am_sure) {
      $pl->e("Refusing to modify partitioning when data in a MAXVALUE partition exists.\n", "Re-run this tool with --i-am-sure if you are sure you want to do this.");
      return undef;
    }
  }

  $last_p->{name} =~ /^$prefix(\d+)$/;
  $next_pN = $1;
  $pl->e("Aborting --add since most recent partition didn't match /^$prefix(\\d+)\$/.")
    and return undef
    if(not defined($next_pN));
  $next_pN++;

  $last_p->{date} = to_date($parts->desc_from_datelike($last_p->{name}));
  $reqdur = DateTime::Duration->new( $range => $add );

  $pl->d('Today:', $today->ymd, 'Last:', $last_p->{date}->ymd);
  $dur = $today->delta_days($last_p->{date});
  $dur = $dur->inverse if($today > $last_p->{date});
  $pl->d("Today - Last:", $dur->in_units('days'), 'days');

  my $r = DateTime::Duration->compare($dur, $reqdur, $today);
  if($r >= 0) {
    $pl->m("At least the requested partitions exist already.\n",
      'Requested out to:', ($today + $reqdur)->ymd(), "\n",
      'Partitions out to:', $last_p->{date}->ymd(), 'exist.');
    $ret = 1;
  }
  else {
    my @part_dates = ();
    my $d2 = $today + $reqdur;
    $pl->d('End date:', $d2->ymd);
    my $du2 = $d2 - $last_p->{date};
    $pl->d('delta pre adjustment:', Dumper($du2));

    unless($uneven) {
      if($range eq 'months' and $du2->in_units('days')) {
        $pl->i("Warning: Rounding up to a full month.");
        push @part_dates, ($last_p->{date} + DateTime::Duration->new(months => 1));
        $du2->subtract(days => $du2->in_units('days'));
      }
      elsif($range eq 'weeks' and $du2->in_units('days') % 7) {
        $pl->i("Warning: Correcting for oddly sized partitioning by creating a slightly larger initial partition.");
        push @part_dates, ($last_p->{date} + DateTime::Duration->new(days => 7 + $du2->in_units('days') % 7));
        $du2->subtract(days => 7 + $du2->in_units('days') % 7);
      }
      elsif($range eq 'days') {
        $du2 = $d2->delta_days($last_p->{date});
      }
      $pl->d('delta post adjustment:',Dumper($du2));
    }

    for(my $i=0; $i < $du2->in_units($range); $i++) {
      my $d;
      if($part_dates[-1]) {
        $d  = $part_dates[-1] + DateTime::Duration->new($range => 1);
      }
      else {
        $d  = $last_p->{date} + DateTime::Duration->new($range => 1);
      }
      push @part_dates, $d;
    }

    if($reorganize) {
      $parts->start_reorganization($parts->last_partition()->{name});
      push @part_dates, 'MAXVALUE';
    }

    $pl->i("Will add", scalar @part_dates, "partitions to satisfy", $add, $range, 'requirement.');

    my $i=0;
    foreach my $date (@part_dates) {
      if($reorganize) {
        if($date eq 'MAXVALUE') {
          $parts->add_reorganized_part($prefix . ($next_pN+$i), $date);
        }
        else {
          $parts->add_reorganized_part($prefix . ($next_pN+$i), $date->ymd);
        }
      }
      else {
        $ret = $parts->add_range_partition($prefix . ($next_pN+$i), $date->ymd, $pretend);
      }
      $i++;
    }

    if($reorganize) {
      $ret = $parts->end_reorganization($pretend);
    }
  }
  return $ret;
}

sub drop_partitions {
  my ($drop, $dbh, $parts, $schema, $table, $i_am_sure) = @_;

  $pl->e("Refusing to drop more than 1 partition unless --i-am-sure is passed.")
    and return undef
    if($drop > 1 and !$i_am_sure);

  my $r = 1;
  $pl->m("Dropping $drop partitions.");
  for(my $i=0; $i < $drop ; $i++) {
    my $p = $parts->first_partition;
    $pl->i("Dropping data older than:", to_date($parts->desc_from_datelike($p->{name}))->ymd);
    $r = $parts->drop_partition($p->{name}, $pretend);
    last unless($r);
  }
  return $r;
}

sub to_date {
  my ($dstr) = @_;
  my $fmt1 = DateTime::Format::Strptime->new(pattern => '%Y-%m-%d', time_zone => 'local');
  my $fmt2 = DateTime::Format::Strptime->new(pattern => '%Y-%m-%d %T', time_zone => 'local');
  return ($fmt1->parse_datetime($dstr) || $fmt2->parse_datetime($dstr))->truncate( to => 'day' );
}

exit main(@ARGV);

=pod

=head1 NAME

pdb-parted - MySQL partition management script

=head1 SYNOPSIS

pdb-parted -d <database> -t <table> [options] [action]

options:

  --host,          -h   Host to connect to.
  --defaults-file, -F   mysql-type config file.
  --prefix,        -P   Partition prefix. Mandatory.
  --range,         -r   Partition range. Mandatory.
                        One of: months, weeks, days.
  --pretend             Report on actions without taking them.

action:

  --add   Add N partitions.
  --drop  Remove N partitions.

=head1 OPTIONS

=over 8

=item --help

This help.

=item --pretend

type: boolean

Report on actions that would be taken. Works best with the C<Pdb_DEBUG> environment variable set to true.

See also: L<ENVIRONMENT>

=item --logfile, -L

type: string

Path to a file for logging, or, C<< syslog:<facility> >>
Where C<< <facility> >> is a pre-defined logging facility for this machine.

See also: L<syslog(3)>, L<syslogd(8)>, L<syslog.conf(5)>

=item --email-to, -E

type: email-address

Where to email failures.

=item --host, -h

type: string

Database host to operate on.

=item --user, -u

type: string

User to connect as.

=item --password, -p

type: string

Password to connect with.

=item --defaults-file, -F

type: path

Path to a my.cnf style config file with user, password, and/or host information.

=item --database, -d

type: string; mandatory

Database schema (database) to operate on.

=item --table, -t

type: string; mandatory

Database table.

=item --prefix, -P

type: string, mandatory

Prefix for partition names. Partitions are always named like: <prefix>N.
Where N is a number.

=item --range, -r

type: string one of: months, weeks, days ; mandatory

This is the interval in which partitions operate. Or, the size of the buckets
that the partitions describe.

=item --i-am-sure

type: boolean

Disables safety for L<"--drop"> and allows dropping more than one partition at a time.

=item --uneven

type: boolean

Allow the tool to possibly add non-whole weeks or months. Has no effect when adding days, as those are the smallest unit this tool supports.

=back

=head1 ACTIONS

=over 8

=item --add

type: integer

Adds partitions till there are at least N L<--range> sized future buckets.

The adding of partitions is not done blindly. This will only add new partitions
if there are fewer than N future partitions. For example, if N is 2 (e.g., C<--add 2> is used),
8 partitions already exist, and today falls in partition 6, then C<--add> will do nothing.

Diagram 1:

  |-----+-|
  0     6 8

Conversely, if N is 3 and the rest of the conditions are as above, then C<--add> will add 1 partition.

Diagram 2:

  |-----+--|
  0     6  9

You can think of C<--add> as specifying a required minimum safety zone.

If L<--uneven> is passed, then this tool will ignore fractional parts of weeks and months.
This can be useful to convert from one size partition to another.
Otherwise, this tool will round up to the largest whole week or month. This means, that if you
are adding monthly partitions, it makes sense to run the tool on the same day of the month.
And, if you are adding weekly partitions, it would behoove you to run this on the same day of the week each time.

=item --drop

type: integer

Drops the N oldest partitions.

B<NOTE:> Unless L<"--i-am-sure"> is passed,
this tool refuses to drop more than 1 at a time.

You'll note from the below diagram that this tool does NOT renumber partitions to start at 0.

Diagram 3:

  Before: |-----+--|
          0     6  9
  After : x-----+--|
           1    6  9

=back

=head1 ENVIRONMENT

Almost all of the PDB (PalominoDB) tools created respond to the environment variable C<Pdb_DEBUG>.
This variable, when set to true, enables additional (very verbose) output from the tools.

=cut

1;
