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
# Lockfile package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End Lockfile package
# ###########################################################################

# ###########################################################################
# CrashReporter package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End CrashReporter package
# ###########################################################################

package pdb_tasks_tracker;
use strict;
use warnings FATAL => 'all';
use POSIX ":sys_wait_h";
use DBI;

use Data::Dumper;
use Time::HiRes qw(gettimeofday tv_interval);
use DateTime;
use IO::Dir;
use File::Spec::Functions;

use Getopt::Long qw(:config no_ignore_case);
use Pod::Usage;

my $dsn;
my %o;

sub get_current_timestamp {
  return DateTime->now(time_zone => 'local')->strftime("%Y-%m-%d %H:%M:%S");
}

sub main {
  my @ARGV = @_;
  %o = ('jobs' => 1);
  my (@files, @commands, $sqldir, $prog_lock);
  GetOptions(\%o,
    'help',
    'sqldir=s',
    'condition=s',
    'dsn=s',
    'stats=s',
    'abort-on-error',
    'logfile|L=s',
    'quiet',
    'dryrun|n',
    'lockfile=s',
    'jobs|j=i'
  );

  if($o{'help'}) {
    pod2usage(-verbose => 1);
  }

  if(!$o{'sqldir'}) {
    pod2usage('Error: --sqldir required');
  }
  if(!$o{'dsn'}) {
    pod2usage("Error: --dsn required");
  }
  if(!$o{'stats'}) {
    pod2usage("Error: --stats required");
  }

  $::PL->logpath($o{'logfile'});
  $::PL->quiet($o{'quiet'});
  $dsn = $o{'dsn'};

  eval {
    $dsn = DSNParser->default()->parse($dsn);
    # Do we have mandatory fields? FIXME
  };
  if($@) {
    pod2usage($@);
  }

  if($o{'condition'}) {
    $::PL->i("Waiting for conditional file", $o{'condition'});
    while(! -f $o{'condition'}) {
      sleep(2);
    }
    # We don't need this file anymore. Minimize
    # The chance of another process coming in behind us and duplicating the work.
    unlink($o{'condition'});
    $::PL->i("Remove condition file", $o{'condition'});
  }

  ## Acquire our main program lock, when we leave main()
  ## this will go out of scope and the lock will be released.
  if($o{'lockfile'}) {
    $prog_lock = Lockfile->get($o{'lockfile'});
  }

  ## Fetch our list of SQL files.
  $sqldir = IO::Dir->new($o{'sqldir'}) or die("Couldn't open $o{'sqldir'}: $!\n");
  while(my $filename = $sqldir->read()) {
    next if $filename eq '.' or $filename eq '..';
    $filename = catfile($o{'sqldir'}, $filename);
    if(! -f $filename or ! -r $filename) {
      $::PL->m("Skipping file [$filename]");
      next;
    }
    push(@files, $filename);
  }
  $sqldir->close();

  $::PL->i("Found", scalar(@files), "command files");
  @files = sort @files; # Sort by filename, natural ordering.

  foreach my $filename (@files) {
    my $fh;
    local $/; # set record separator to undef to enable slurp.
    open($fh, "<$filename") || die("Failed to open [$filename]");
    $_ = <$fh>;
    if(/^[\s\n\t]*$/) {
      $::PL->i("Skipping file [$filename]. No content.");
      next;
    }
    push(@commands, {
                      'filename' => $filename,
                      'commands' => $_,
                    }
        );

  }

  ## Test connectivity to target, and stats connections
  ## before attempting to execute any SQL. Early failure is good for you.
  eval {
    my $dbh = $dsn->get_dbh(0);
    $dbh->disconnect();
    my $sdsn = DSNParser->default()->parse($o{'stats'});
    $sdsn->fill_in($dsn);
    $dbh = $sdsn->get_dbh();
  };
  if($@) {
    pod2usage($@);
  }

  $::PL->i("Will write results to ".$o{'stats'});

  my $done = 0;
  my $drain_and_exit = 0;
  my %children;
  $::PL->i("Processing ".scalar(@commands)." commands");
  while(!$done) {
    $::PL->i("In Queue [".scalar(@commands)."] In Flight [".scalar(keys %children)."]");
    if(!$drain_and_exit && scalar(@commands) > 0 && scalar(keys %children) < $o{'jobs'}) {
      # Fork off a new one
      my $pid = fork();
      my $command_data = shift(@commands);
      if($pid) {
        # Parent
        $children{$pid} = 1;
      }
      else {
        # Child
        my $dbh = $dsn->get_dbh(1);
        my $had_error = 0;
        my $i = 0;
        my ($status, $rows, $start, $elapsed, $t0, @warnings)
          = ('', 0, get_current_timestamp(), 0.0, [gettimeofday()], ());

        ## Split queries based on something that Probably will never occur.
        my @cmds = split(/\/\/# ###########################################################################/,
            $command_data->{'commands'}
          );

        foreach my $cmd (@cmds) {
          eval {
            $dbh->{AutoCommit} = 0;
            # Execute the query
            $::PL->i("Executing [$cmd]\n",
                     "from file [".$command_data->{'filename'}."]");
            $rows = $dbh->do($cmd);
            $rows = 0 if $rows eq '0E0';

            my $sth = $dbh->prepare('SHOW WARNINGS');
            $sth->execute();
            while(my $wa = $sth->fetchrow_hashref) {
              push(@warnings, "$wa->{'Message'} ($wa->{'Code'})");
            }

            $dbh->commit();
          };

          $status = "$@";

          $elapsed = tv_interval($t0);
          if($status) {
            $had_error = 1;
            $::PL->e("Command [$cmd]\n",
                     "returned error [".$status."] after",
                     $elapsed, "seconds.");
          }

          # Our results
          save_statistics(
            $command_data->{'filename'} .':'. $i,
            $start,
            $elapsed,
            $rows,
            $status,
            join("\n", @warnings)
          );
          $i++;
        }
        $dbh->disconnect();

        exit($had_error);
      }
    }

    if(scalar(keys %children) > 0) {
      # See if any children have finished.
      my $stiff = waitpid(-1, &WNOHANG);
      if($stiff == 0) {
        # Nothing finished yet, take a break
        sleep(2);
      }
      else {
        my $exit_value = $? >> 8;
        if($exit_value == 0) {
          # Finished without problems
        }
        else {
          if($o{'abort-on-error'}) {
            # Wait for the children in progress and exit without spawning new ones
            $::PL->e("Caught an error. Will wait for in-flight processes to finish and then will abort processing");
            $drain_and_exit = 1;
          }
        }
        $::PL->i("Process [".$stiff."] finished with exit code [".$exit_value."]");
        delete $children{$stiff};
      }
    }
    elsif($drain_and_exit or scalar(@commands) == 0) {
      # We are done
      $done = 1;
    }
  }

  return 0;
}

sub save_statistics {
  my (@args) = @_;
  eval {
    my ($dbh, $tbl);
    my $sdsn = DSNParser->default()->parse($o{'stats'});
    $sdsn->fill_in($dsn);
    $dbh = $sdsn->get_dbh();
    $tbl = $dbh->quote_identifier(undef, $sdsn->get('D'), $sdsn->get('t'));

    $dbh->do(
      qq|INSERT INTO $tbl (`file`, `start_time`, `elapsed`, `rows`, `error`, `warnings`)
          VALUES (?, ?, ?, ?, ?, ?)|,
      undef,
      @args
    );
    $dbh->commit();
    $dbh->disconnect();
  };
  if($@) {
    $::PL->e('Error saving statistics:', "$@");
  }
}

if(!caller) { CrashReporter->install(); exit(main(@ARGV)); }

=pod

=head1 NAME

pdb-tasks-tracker - run tasks and compile information about them

=head1 EXAMPLES

  # Run 4 concurrent jobs out of sql_1/
  pdb-tasks-tracker --sqldir sql_1/ --dsn h=localhost \
                    --stats D=stats,t=tasks --jobs 4

=head1 SYNOPSIS

pdb-tasks-tracker [-h] --sqldir DIR --dsn DSN --stats

Run with -h or --help for options.

=head2 OPTIONS

=over 8

=item --help

This help.

Help is most awesome. It's like soap for your brain.

=item --logfile,-L

Path to a file for logging, or, C<< syslog:<facility> >>
Where C<< <facility> >> is a pre-defined logging facility for this machine.

See also: L<syslog(3)>, L<syslogd(8)>, L<syslog.conf(5)>

=item --quiet

Suppress output to the console.

=item --condition

File to read commands from. This script will sit in a sleep loop until this
file become available. B<NOTE:> This script will B<REMOVE> the condition file
upon picking it up, so, do not set this to a data file.

=item --dsn

DSN of MySQL server to perform commands against.

=item --stats

DSN for where to store SQL run statistics. This need only contain the
D and t keys, since, missing values will be filled in from --dsn.

The table that the results are inserted into must have the form, or a form
compatible with:

  CREATE TABLE `task_stats` (
    `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
    `file` VARCHAR(64) NOT NULL,
    `start_time` DATETIME NOT NULL,
    `elapsed` DECIMAL(10,4) NOT NULL,
    `rows` INTEGER NOT NULL,
    `error` TEXT NOT NULL,
    `warnings` TEXT NOT NULL,
    UNIQUE KEY `file_start` (`file`, `start_time`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

=item --sqldir

Directory of SQL statements to run.
If you need to enforce some ordering, prefix the files with NN_.
This tool will sort the files the way you expect ( 02 < 10 ).

=item --jobs,-j

Number of concurrent SQL queries to run.
The default number of jobs to run is 1, which means: not parallel.

=item --abort-on-error

Processing will immediately stop if an error is encountered.
Normally, this tool will run all available SQL regardless of errors, so
as to collect as much information about the failure as possible (in theory).

=item --lockfile

Acquire flock the given file to ensure that multiple concurrent runs of this
tool do not occur.

=back

=head2 DSN KEYS

All of the standard keys apply. Here a few samples:

  h=localhost,u=root,p=pass,S=/tmp/my.sock,D=db,t=tbl
  h=remotehost,u=bill,p=Zork,N=utf8
  F=params.cnf,h=host

=cut

1;




