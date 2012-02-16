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
use warnings FATAL => 'all';

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

# ###########################################################################
# IniFile package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End IniFile package
# ###########################################################################

package pdb_parted;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);

use ProcessLog;
use IniFile;
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
    $do_archive,
    $older_than
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
    "prefix|P=s" => \$prefix,
    "range|r=s" => \$range,
    "older-than=s" => \$older_than,
    "add=i" => \$add,
    "drop=i" => \$drop,
    "archive" => \$do_archive,
    "i-am-sure" => \$i_am_sure,
    "uneven" => \$uneven
  );

  unless($db_schema and $db_table and $prefix and $range) {
    pod2usage(-message => "--table, --database, --prefix and --range are mandatory.");
  }

  unless($range eq 'months' or $range eq 'days' or $range eq 'weeks') {
    pod2usage(-message => "--range must be one of: months, days, or weeks.");
  }

  $range=lc($range);

  unless($prefix =~ /^[A-Za-z][A-Za-z0-9_-]*$/) {
    pod2usage(-message => "--prefix ($prefix) must not include non alpha-numeric characters.");
  }

  unless($add or $drop) {
    pod2usage(-message => "--add or --drop required.");
  }

  if($db_file and ! -f $db_file) {
    pod2usage(-message => "--defaults-file $db_file doesn't exist, or is a directory.");
  }

  if(!$db_user and $db_file) {
    my $inf = IniFile->read_config($db_file);
    $db_host ||= $inf->{client}->{host};
    $db_user ||= $inf->{client}->{user};
    $db_pass ||= $inf->{client}->{password};
  }

  # Pretty safe to assume localhost if not set.
  # Most MySQL tools do.
  $db_host ||= 'localhost';

  if($add and $drop) {
    pod2usage(-message => "only one of --add or --drop may be specified");
  }
  if($range and $older_than) {
    pod2usage(-message => "only one of --range or --older-than may be specified.");
  }

  if($older_than) {
    unless(($older_than = to_date($older_than))) {
      pod2usage(-message => "--older-than must be in the form YYYY-MM-DD.");
    }
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
    $r = add_partitions($add, $dbh, $parts, $prefix, $range, $db_host, $db_schema, $db_table, $i_am_sure);
  }
  elsif($drop) {
    $r = drop_partitions($drop, $dbh, $parts, $range, $older_than,
    $db_host, $db_schema, $db_table, $db_user, $db_pass,
    $db_file, $i_am_sure, $do_archive);
  }

  $dbh->disconnect;
  $pl->failure_email() unless($r);
  $pl->end;
  return 0;
}

sub add_partitions {
  my ($add, $dbh, $parts, $prefix, $range, $db_host, $db_schema, $db_table, $i_am_sure) = @_;

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
    my $end_date = $today + $reqdur;
    my $curs_date = $last_p->{date};

    $pl->d('End date:', $end_date->ymd);

    ###########################################################################
    # Handle the case where we aren't run on the same day of the week or month.
    # This is/was part of the requirements for the pdb-parted tool.
    # It used to be that this code would try to add an extra partition in to 
    # compenstate for the offset. The new code just fiddles with the start date
    # to get it to land on a multiple of $today*$range.
    ###########################################################################
    $pl->d('Checking for uneven partitioning.');
    if($range eq 'months') {
      my $uneven_dur = $today->delta_md($last_p->{date});
      $pl->d(Dumper($uneven_dur));
      if($uneven_dur->delta_days) {
        $pl->i('Found uneven partitioning.', $uneven_dur->delta_days, 'days. Are you running on the same day of the month?');
        unless($uneven) {
          $curs_date->add('days' => $uneven_dur->delta_days) unless($uneven);
        }
      }
    }
    elsif($range eq 'weeks') {
      my $uneven_dur = $today->delta_days($last_p->{date});
      $pl->d(Dumper($uneven_dur));
      if($uneven_dur->delta_days % 7) {
        $pl->i('Found uneven partitioning.', 7 - $uneven_dur->delta_days % 7, 'days. Are you running on the same day of the week?');
        unless($uneven) {
          $curs_date->subtract('days' => 7 -  $uneven_dur->delta_days % 7);
        }
      }
    }

    $pl->d('cur date:', $curs_date->ymd);

    ###########################################################################
    # Just loop until $curs_date (date cursor) is greater than
    # where we want to be. We advance the cursor by $range increments.
    ###########################################################################
    while($curs_date < $end_date) {
      push @part_dates, $curs_date->add($range => 1)->clone();
    }

    $pl->d(Dumper([ map { $_->ymd } @part_dates]));

    if($reorganize) {
      $parts->start_reorganization($parts->last_partition()->{name});
      push @part_dates, 'MAXVALUE';
    }

    $pl->i("Will add", scalar @part_dates, "partitions to satisfy", $add, $range, 'requirement.');

    my $i=0;
    ###########################################################################
    # Loop over the calculated dates and add partitions for each one
    ###########################################################################
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
  my ($drop, $dbh, $parts, $range, $older_than, $host, $schema,
     $table, $user, $pw, $dfile, $i_am_sure, $do_archive) = @_;

  my $today = DateTime->today(time_zone => 'local');
  $pl->e("Refusing to drop more than 1 partition unless --i-am-sure is passed.")
    and return undef
    if($drop > 1 and !$i_am_sure);

  # Return value of this subroutine.
  my $r = 1;
  $pl->m("Dropping $drop partitions.");
  for(my $i=0; $i < $drop ; $i++) {
    my $p = $parts->first_partition;
    my $p_date = to_date($parts->desc_from_datelike($p->{name}));
    ## Determine if the partition is within $range or $older_than
    if($range) {
      if($p_date > $today->clone()->subtract($range => $drop)) {
        $pl->d("Skipping $p->{name} @ $p_date");
        next;
      }
    }
    elsif($older_than) {
      if($p_date > $older_than) {
        $pl->d("Skipping $p->{name} @ $p_date");
        next;
      }
    }
    if($do_archive) {
      archive_partition($parts, $p, $host, $schema, $table, $user, $pw, $dfile);
    }
    $pl->i("Dropping data older than:", $p_date);
    $r = $parts->drop_partition($p->{name}, $pretend);
    last unless($r);
  }
  return $r;
}

sub archive_partition {
  my ($parts, $part, $host, $schema, $table, $user, $pw, $dfile) = @_;
  my ($desc, $fn, $cfn) = $parts->expr_datelike();
  if($cfn) {
    $desc = "$cfn(". $part->{description} . ")";
  }
  else {
    $desc = $part->{description};
  }
  my @dump_EXEC = ("mysqldump",
      ( $dfile ? ("--defaults-file=$dfile") : () ),
      "--no-create-info",
      "--result-file=". "$host.$schema.$table.". $part->{name} . ".sql",
      ($host ? ("-h$host") : () ),
      ($user ? ("-u$user") : () ),
      ($pw ? ("-p$pw") : () ),
      "-w ". $parts->expression_column() . "<$desc",
      $schema,
      $table);
  $pl->i("Archiving:", $part->{name}, "to", "$host.$schema.$table". $part->{name} . ".sql");
  $pl->d("Executing:", @dump_EXEC);
  unless($pretend) {
    system(@dump_EXEC);
  }
  else {
    $? = 0;
  }
  if(($? << 8) != 0) {
    $pl->e("Failed to archive $schema.$table.". $part->{name}, "got:", ($? << 8), "from mysqldump");
    die("Failed to archive $schema.$table.". $part->{name})
  }
}

sub to_date {
  my ($dstr) = @_;
  #############################################################################
  # MySQL can return two different kinds of dates to us.
  # For DATE columns we just get the date. Obviously.
  # For virtually all other time related columns, we also get a time.
  # This method first tries parsing with just dates and then tries with time.
  #############################################################################
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
  
  --archive             Archive partitions before dropping them.

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

When adding paritions, it specifies what timeframe the partitions describe.

When dropping partitions, it specifies the multiplier for the N in C<--drop=N>.
So, if you have: C<--range weeks --drop 3>, you're asking to drop data older than
three weeks.

B<Note that you'll also have to pass C<--i-am-sure> in order to drop
more than one partition.>

=item --i-am-sure

type: boolean

Disables safety for L<"--drop"> and allows dropping more than one partition at a time.

=item --uneven

type: boolean

Allow the tool to possibly add non-whole weeks or months. Has no effect when adding days, as those are the smallest unit this tool supports.

=item --archive

type: boolean

mysqldump partitions to files B<in the current directory> named like <host>.<schema>.<table>.<partition_name>.sql

There is not currently a way to archive without dropping a partition.

=item --older-than

type: date

For dropping data, this is an alternate to L<--range>. It specifies
an absolute date for which partitions older than it should be dropped.
The date B<MUST> be in the format: C<YYYY-MM-DD>. 

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
