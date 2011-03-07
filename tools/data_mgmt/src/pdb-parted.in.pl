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
# ProcessLog package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessLog package
# ###########################################################################

# ###########################################################################
# DSN package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End DSN package
# ###########################################################################

# ###########################################################################
# TablePartitions package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End TablePartitions package
# ###########################################################################

# ###########################################################################
# Timespec package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End Timespec package
# ###########################################################################

# ###########################################################################
# IniFile package FSL_VERSION
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
use DSN;
use Timespec;

use DBI;
use Getopt::Long qw(:config no_ignore_case pass_through);
use Pod::Usage;
use DateTime;
use DateTime::Duration;
use DateTime::Format::Strptime;

use Data::Dumper;
{
  no warnings 'once';
  $Data::Dumper::Indent = 0;
  $Data::Dumper::Sortkeys = 1;
}

my $PL = $::PL ? $::PL : ProcessLog->new($0, '/dev/null');

my %o = (
  prefix  => 'p',
  logfile => '/dev/null',
);

sub main {
  @ARGV = @_;
  my (
    $r,
    $dsn,
    $parts,
    $timespec,
    $requested_dt,
    $email_log,
    @partitions
  );

  GetOptions(\%o,
    "help|h",
    "dryrun|n",
    "logfile|L=s",
    "quiet|q",
    "email-to|E=s",
    "email-activity",
    "prefix|P=s",
    "interval|i=s",
    "limit=i",
    "add",
    "drop",
    "archive",
    "archive-path=s",
    "i-am-sure",
  );

  $timespec = shift @ARGV;
  $dsn    = shift @ARGV;
  pod2usage("Missing TIMESPEC") if(!$timespec);
  pod2usage("Missing DSN") if(!$dsn);

  $email_log    = '';
  eval {
    $requested_dt = Timespec->parse($timespec);
    $dsn          = DSNParser->default()->parse($dsn);
  };
  if($@) {
    pod2usage($@);
  }

  unless($o{'drop'}) {
    # interval is not necessary for --drop.
    unless($o{'interval'} and $o{'interval'} =~ /^[hdwmqy]$/) {
      pod2usage("interval must be one of: h,d,w,m,q,y");
    }
  }

  unless($o{'prefix'} =~ /^[A-Za-z][A-Za-z0-9_-]*$/) {
    pod2usage("--prefix ($o{'prefix'}) must not include non alpha-numeric characters.");
  }

  unless($o{'add'} or $o{'drop'}) {
    pod2usage("ACTION required");
  }

  if($o{'add'} and $o{'drop'}) {
    pod2usage("Cannot perform more than one action at once");
  }

  if($o{'email-activity'} and !$o{'email-to'}) {
    pod2usage("--email-activity can only be used with --email-to.");
  }

  $PL->start();
  $PL->email_to($o{'email-to'});
  $parts = TablePartitions->new($PL, $dsn);

  if($o{'add'}) {
    $email_log = "Adding partitions to ". $dsn->get('h') .
      "." . $dsn->get('D') . "." . $dsn->get('t') . ":\n";
    my $last_p = $parts->last_partition;
    my $last_d = to_date($parts->desc_from_datelike($last_p->{name}));
    if($last_d >= $requested_dt) {
      $PL->m("At least the requested partitions exist already.\n",
             'Requested out to:', $requested_dt->ymd(), "\n",
             'Partitions out to:', $last_d->ymd(), 'exist.');
      $r = 0; # success
      goto DONE;
    }
    eval {
      @partitions = add_partitions($dsn, $parts, $requested_dt, %o);
      $r = 0;
    };
    if($@) {
      $_ = "$@";
      $PL->e("Error adding partitions:", $_);
      $r = 1;
      goto DONE;
    }

    if($o{'email-activity'}) {
      for(@partitions) {
        $email_log .= "- $_->{name} [older than: $_->{date}]\n";
      }
      $PL->send_email("Partitions added on ". $dsn->get('h') .
                      "." . $dsn->get('D') . "." . $dsn->get('t'), $email_log);
    }
  }
  elsif($o{'drop'}) {
    $email_log = "Dropped partitions from ". $dsn->get('h') .
      "." . $dsn->get('D') . "." . $dsn->get('t') . ":\n";

    eval {
      @partitions = drop_partitions($dsn, $parts, $requested_dt, %o);
      $r = 0;
    };
    if($@) {
      $_ = "$@";
      $PL->e("Error dropping partitions:", $_);
      $r = 1;
      goto DONE;
    }

    if($o{'email-activity'}) {
      for(@partitions) {
        $email_log .= "- $_->{name} [older than: $_->{date}]\n";
      }
      $PL->send_email("Partitions dropped on ". $dsn->get('h') .
                      "." . $dsn->get('D') . "." . $dsn->get('t'), $email_log);
    }
  }

  DONE:
  $PL->failure_email() if($r);
  $PL->end();
  return $r;
}

sub interval {
  my $interval = shift;
  my %i = ( 'h' => 'hours', 'd' => 'days', 'w' => 'weeks',
            'm' => 'months', 'y' => 'years' );
  if($interval eq 'q') {
    return DateTime::Duration->new( months => 3 );
  }
  return DateTime::Duration->new( $i{$interval} => 1 );
}

sub add_partitions {
  my ($dsn, $parts, $end_date, %o) = @_;
  my $dbh = $dsn->get_dbh(1);
  die("missing mandatory argument prefix\n") unless($o{'prefix'});
  my $db_host = $dsn->get('h');
  my $db_schema = $dsn->get('D');
  my $db_table = $dsn->get('t');
  my $prefix = $o{'prefix'};
  my $i_am_sure = $o{'i-am-sure'};
  my $dryrun = $o{'dryrun'};
  my $interval = interval($o{'interval'});
  my @parts = ();

  my $i = 0;
  my $ret = 0;
  my $last_p = $parts->last_partition;
  my $next_pN = undef;
  my $curs_date = undef;

  my $reorganize = uc($last_p->{description}) eq 'MAXVALUE';

  if ($reorganize) {
    $last_p = $parts->partitions()->[-2];
    if ($parts->has_maxvalue_data and !$i_am_sure) {
      die("Data in MAXVALUE partition exists.\n");
    }
  }

  $last_p->{name} =~ /^$prefix(\d+)$/;
  $next_pN = $1;
  die("most recent partition didn't match /^$prefix(\\d+)\$/.\n")
    if (not defined($next_pN));
  $next_pN++;

  $last_p->{date} = to_date($parts->desc_from_datelike($last_p->{name}));
  $curs_date = $last_p->{date};

  $PL->d('Last partition:', $last_p->{date}->ymd);
  $PL->d('End date:', $end_date->ymd);

  ###########################################################################
  # Just loop until $curs_date (date cursor) is greater than
  # where we want to be. We advance the cursor by $range increments.
  ###########################################################################
  $i = 0;
  while ($curs_date < $end_date) {
    last if($o{'limit'} and $i >= $o{'limit'});
    push(@parts, {
      name => "$prefix". ($next_pN+$i),
      date => $curs_date->add_duration($interval)->clone(),
    });
    $i++;
  }

  $PL->i('Will add', scalar @parts, 'partition(s).', "\n",
         "Partitions: ",
         join(', ', map { "$_->{name}($_->{date})" } @parts), "\n");

  if ($reorganize) {
    $parts->start_reorganization($parts->last_partition()->{name});
    push(@parts, { name => "$prefix". ($next_pN+$i), date => 'MAXVALUE' });
  }

  ###########################################################################
  # Loop over the calculated dates and add partitions for each one
  ###########################################################################
  foreach my $part (@parts) {
    my $name = $part->{name};
    my $date = $part->{date};
    if ($reorganize) {
      if ($date eq 'MAXVALUE') {
        $parts->add_reorganized_part($part->{name}, $date);
      } else {
        $parts->add_reorganized_part($part->{name}, $date->ymd);
      }
    } else {
      $ret = $parts->add_range_partition($part->{name}, $date->ymd, $dryrun);
      if(!$ret) {
        die("$part->{name} $part->{date}\n");
      }
    }
  }

  if ($reorganize) {
    $ret = $parts->end_reorganization($dryrun);
    if(!$ret) {
      die("re-organizing\n");
    }
  }

  return @parts;
}

sub drop_partitions {
  my ($dsn, $parts, $requested_dt, %o) = @_;
  my @drops;
  foreach my $part (@{$parts->partitions()}) {
    $part->{date} = to_date($parts->desc_from_datelike($part->{name}));
    if($part->{date} < $requested_dt) {
      push @drops, $part;
    }
    last if($o{'limit'} and scalar @drops >= $o{'limit'});
  }

  $PL->i('Will drop', scalar @drops, 'partition(s).', "\n",
         "Partitions: ",
         join(', ', map { "$_->{name}($_->{date})" } @drops), "\n");

  foreach my $part (@drops) {
    if($o{'archive'}) {
      archive_partition($dsn, $parts, $part, %o);
    }
    if(!$parts->drop_partition($part->{name}, $o{'dryrun'})) {
      die("$part->{name} $part->{date}");
    }
  }
  return @drops;
}

sub archive_partition {
  my ($dsn, $parts, $part, %o) = @_;
  my $path = $o{'archive-path'} || "";
  if($path) {
    $path =~ s/[^\/]$/\//;
  }
  my $host = $dsn->get('h');
  my $user = $dsn->get('u');
  my $pw = $dsn->get('p');
  my $schema = $dsn->get('D');
  my $table = $dsn->get('t');

  my $dfile = $dsn->get('F');
  my $r;

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
                   "--result-file=". "${path}$host.$schema.$table.". $part->{name} . ".sql",
                   ($host ? ("-h$host") : () ),
                   ($user ? ("-u$user") : () ),
                   ($pw ? ("-p$pw") : () ),
                   "-w ". $parts->expression_column() . "<$desc",
                   $schema,
                   $table);
  $PL->i("Archiving:", $part->{name}, "to", "${path}$host.$schema.$table.". $part->{name} . ".sql");
  $PL->d("Executing:", @dump_EXEC);
  unless($o{'dryrun'}) {
    $r = $PL->x(sub { system(@_) }, @dump_EXEC);
  }
  else {
    $r = { rcode => 0, error => '', fh => undef };
  }
  if(($$r{rcode} >> 8) != 0) {
    $_ = $$r{fh};
    while (<$_>) { $PL->e($_); }
    $PL->e("got:", ($$r{rcode} >> 8), "from mysqldump.");
    die("archiving $host.$schema.$table.$part->{name}\n");
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


if(!caller) { exit(main(@ARGV)); }

=pod

=head1 NAME

pdb-parted - MySQL partition management script

=head1 EXAMPLES

  # Create weekly partitions for the next quarter to test.part_table
  pdb-parted --add --interval w +1q h=localhost,D=test,t=part_table

  # Create daily partitions for the next 2 weeks
  # starting exactly at the beginning of every day
  pdb-parted --add --interval d +2w.startof h=localhost,D=test,t=part_table

  # Email ops@example.com about partitions added
  pdb-parted --add --email-activity --email-to ops@example.com \
             --interval d +4w h=localhost,D=test,t=part_table

  # Drop partitions older than 8 weeks
  pdb-parted --drop -8w h=localhost,D=test,t=part_table

  # Drop partitions older than Dec 20th, 2010, but only 5 of them.
  pdb-parted --drop --limit 5 '2010-12-20 00:00:00' \
             h=localhost,D=test,t=part_table

  # Drop and archive partitions older than 2 quarters ago.
  pdb-parted --drop --archive --archive-path /backups -2q \
             h=locahost,D=test,t=part_table

  # Logging to syslog
  pdb-parted --logfile syslog:LOCAL0 --add --interval d 1y \
             h=localhost,D=test,t=part_table


=head1 SYNOPSIS

pdb-parted [options] ACTION TIMESPEC DSN

This tool assists in the creation of partitions in regular intervals.
It creates partitions in regular intervals up to some maximum future date.

  --help,          -h   This help. See C<perldoc pdb-parted> for full docs.
  --dryrun,        -n   Report on actions without taking them.
  --logfile,       -L   Direct output to given logfile. Default: none.

  --email-activity      Send a brief email report of actions taken.
                        The email is sent to --email-to.
  --email-to,      -E   Where to send activity and failure emails.
                        Default: none.

  --prefix,        -P   Partition prefix. Defaults to 'p'.

  --archive             Archive partitions before dropping them.
  --archive-path        Directory to place mysqldumps.
                        Default: current directory.

  --limit,         -m   Limit the number of actions to be performed.
                        Default: 0 (unlimited)

=head2 ACTION

  --add   Add partitions.
  --drop  Remove partitions.

=head2 TIMESPEC

A timespec is a "natural" string to specify how far in advance to create
partitions. A sampling of possible timespecs:

  1w (create partitions one week in advance)
  1m (one month)
  2q (two quarters)
  5h (five hours)

See the full documentation for a complete description of timespecs.

=head2 DSN

DSNs, such as those passed as option values, or arguments to a program
are of the format: C<({key}={value}(,{key}={value})*>. That is, a C<key=value> pair, followed
by a comma, followed by any number of additional C<key=value> pairs separated by
commas.

Examples:

  h=testdb1,u=pdb,p=frogs
  h=localhost,S=/tmp/mysql.sock,u=root,F=/root/my.cnf

Where 'h' is a hostname, 'S' is a socket path, 'u' is a user, 'F' is a path
to a defaults file, and 'p' is a password. These are non-exhaustive examples.

=head1 TIMESPEC

A timespec is one of:

  A modifier to current local time,
  A unix timestamp (assumed in UTC),
  The string 'now' to refer to current local time,
  An absolute time in 'YYYY-MM-DD HH:MM:SS' format,
  An absolute time in 'YYYY-MD-DD HH:MM:SS TIMEZONE' format.

For the purposes of this module, TIMEZONE refers to zone names
created and maintained by the zoneinfo database.
See L<http://en.wikipedia.org/wiki/Tz_database> for more information.
Commonly used zone names are: Etc/UTC, US/Pacific and US/Eastern.

Since the last four aren't very complicated, this section describes
what the modifiers are.

A modifer is, an optional plus or minus sign followed by a number,
and then one of:

  y = year, q = quarter , m = month, w = week, d = day, h = hour

Followed optionally by a space or a period and 'startof'.
Which is described in the next section.

Some examples (the time is assumed to be 00:00:00):

  -1y         (2010-11-01 -> 2009-11-01)
   5d         (2010-12-10 -> 2010-12-15)
  -1w         (2010-12-13 -> 2010-12-07)
  -1q startof (2010-05-01 -> 2010-01-01)
   1q.startof (2010-05-01 -> 2010-07-01)

=head2 startof

The 'startof' modifier for timespecs is a little confusing,
but, is the only sane way to achieve latching like behavior.
It adjusts the reference time so that it starts at the beginning
of the requested type of interval. So, if you specify C<-1h startof>,
and the current time is: C<2010-12-03 04:33:56>, first the calculation
throws away C<33:56> to get: C<2010-12-03 04:00:00>, and then subtracts
one hour to yield: C<2010-12-03 03:00:00>.

Diagram of the 'startof' operator for timespec C<-1q startof>,
given the date C<2010-05-01 00:00>.

          R P   C
          v v   v
   ---.---.---.---.---.--- Dec 2010
   ^   ^   ^   ^   ^   ^
   Jul Oct Jan Apr Jul Oct
  2009    2010

  . = quarter separator
  C = current quarter
  P = previous quarter
  R = Resultant time (2010-01-01 00:00:00)

=head1 OPTIONS

=over 8

=item --help, -h

This help.

=item --dryrun, -n

Report on actions that would be taken. Works best with the C<Pdb_DEBUG> environment variable set to true.

See also: L<ENVIRONMENT>

=item --logfile, -L

Path to a file for logging, or, C<< syslog:<facility> >>
Where C<< <facility> >> is a pre-defined logging facility for this machine.

See also: L<syslog(3)>, L<syslogd(8)>, L<syslog.conf(5)>

=item --email-to, -E

Where to send emails.

This tool can send emails on failure, and whenever it adds, drops, or archive partitions.
Ordinarily, it will only send emails on failure.

=item --email-activity

If this flag is present, then this will make the tool also email
whenver it adds, drops, or archives a partition.

=item --prefix, -P

Prefix for partition names. Partitions are always named like: <prefix>N.
Where N is a number. Default is 'p', which was observed to be the most common prefix.

=item --interval, -i

type: string one of: d w m y

Specifies the size of the each partition for the --add action.
'd' is day, 'w' is week, 'm' is month, and 'y' is year.

=item --limit

Specifies a limit to the number of partitions to add, drop, or archive.
By default this is unlimited (0), so, for testing one usually wishes to set
this to 1.

=item --archive

type: boolean

mysqldump partitions to files B<in the current directory> named like <host>.<schema>.<table>.<partition_name>.sql

There is not currently a way to archive without dropping a partition.

=item --archive-path

What directory to place the SQL dumps of partition data in.

=back

=head1 ACTIONS

=over 8

=item --add

Adds partitions till there are at least TIMESPEC L<--interval> sized future buckets.

The adding of partitions is not done blindly. This will only add new partitions
if there are fewer than TIMESPEC future partitions. For example:

  Given: --interval d, today is: 2011-01-15, TIMESPEC is: +1w,
         last partition (p5) is for 2011-01-16;

  Result:
    Parted will add 6 partitions to make the last partition 2011-01-22 (p11).

  Before:
   |---+|
  p0  p5

  After:
   |---+-----|
  p0  p5    p11

You can think of C<--add> as specifying a required minimum safety zone.

=item --drop

Drops partitions strictly older than TIMESPEC.
The partitions are not renumbered to start with p0 again.

  Given: today is: 2011-01-15, TIMESPEC is: -1w,
         first partition (p0) is for 2011-01-06


  Result: 2 partitions will be dropped.

  Before: |-----+--|
          0     6  9
  After : |---+--|
          2   6  9

=back

=head1 HISTORY

Previous versions of this tool took complicated and error-prone steps
to normalize the ending date to be exactly on the requested date. This
would result in oddly sized partitions being added if the tool wasn't
run on the same day of the week or month.

This version no longer performs those steps and instead adds exactly
sized partitions starting from the last partition on a table until
there are partitions to cover at least the requested end date. If the
partitions run over that date, it's considered unimportant.

=head1 ENVIRONMENT

Almost all of the PDB (PalominoDB) tools created respond to the environment variable C<Pdb_DEBUG>.
This variable, when set to true, enables additional (very verbose) output from the tools.

=cut

1;
