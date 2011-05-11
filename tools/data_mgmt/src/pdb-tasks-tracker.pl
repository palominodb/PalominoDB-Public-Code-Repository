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
# ProcessLog package GIT_VERSION
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

package pdb_tasks_tracker;
use strict;
use POSIX ":sys_wait_h";
use warnings FATAL => 'all';
use DBI;
use ProcessLog;
use DSN;
use Data::Dumper;
use Time::HiRes qw(gettimeofday tv_interval);

use Getopt::Long;
use Pod::Usage;

my $pl; # ProcessLog.
my $dbh;
my $dsn;
my %o;
my $quiet = 0;
my $sqldir;
my $condition_filename;
my $output_filename = './output.'.$$.'.dat';
my $abort_on_error = 0;

my %results;
my $email = undef;

sub get_current_timestamp {
  my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime();
  $year += 1900;
  $mon  += 1;
  return sprintf('%d-%0.2d-%0.2d %0.2d:%0.2d:%0.2d', $year, $mon, $mday, $hour, $min, $sec);
}

sub main {
  my @ARGV = @_;
  GetOptions(\%o,
    'help',
    'sqldir=s',
    'condition=s',
    'dsn=s',
    'abort-on-error',
    'logfile',
    'quiet',
  );

  if($o{'help'}) {
    pod2usage();
  }

  if(!$o{'sqldir'}) {
    pod2usage('sqldir required');
  }
  $sqldir = $o{'sqldir'};

  if($o{'logfile'}) {
    $::PL->logpath($o{'logfile'});
  }

  if($o{'quiet'}) {
    $quiet = 1;
  }

  if($o{'abort-on-error'}) {
    $abort_on_error = 1;
  }

  if($o{'condition'}) {
    $condition_filename = $o{'condition'};
  }

  if(! $o{'dsn'}) {
    pod2usage("DSN required");
  }
  $dsn = $o{'dsn'};

  eval {
    $dsn          = DSNParser->default()->parse($dsn);
    # Do we have mandatory fields? FIXME
  };
  if($@) {
    pod2usage($@);
  }

  if($condition_filename) {
    $::PL->i("Waiting for conditional file ".$condition_filename);
    while(! -f $condition_filename) {
      sleep(2);
    }
    # We don't need this file anymore. Minimize
    # The chance of another process coming in behind us and duplicating the work.
    unlink($condition_filename);
    $::PL->i("Remove condition file $condition_filename") unless $quiet;
  }

  my @files;
  opendir(DIR, $sqldir) or die("Couldn't open $sqldir: $!\n");
  while(my $filename = readdir(DIR)) {
    $filename = $sqldir.'/'.$filename;
    next if $filename eq '.' or $filename eq '..';
    if(! -f $filename or ! -r $filename) {
      $::PL->e("Skipping file [$filename]") unless $quiet;
      next;
    }
    push(@files, $filename);
  }
  close(DIR);

  $::PL->i("Found ".scalar(@files)." command files");

  @files = sort @files; # Sort by filename, natural ordering.

  my @commands;
  foreach my $filename (@files) {
    open(FILE, "<$filename") || die("Failed to open [$filename]");
    my @data = <FILE>;
    close(FILE);
    my $command = join('', @data);
    if($command =~ /^[\s\n\t]*$/) {
      $::PL->i("Skipping file [$filename]. No content") unless $quiet;
      next;
    }
    push(@commands, { 'filename' => $filename,
                      'command' => $command,
                      }
        );
  }

  $::PL->i("Will write results to ".$output_filename);
  open(OUTPUTFILE, ">$output_filename") or die("Couldn't open $output_filename");

  my $done = 0;
  my $drain_and_exit = 0;
  my $max_children = 4;
  my %children;
  $::PL->i("Processing ".scalar(@commands)." commands");
  while(!$done) {
    $::PL->i("In Queue [".scalar(@commands)."] In Flight [".scalar(keys %children)."]") unless $quiet;
    if(!$drain_and_exit && scalar(@commands) > 0 && scalar(keys %children) < $max_children) {
      # Fork off a new one
      my $pid = fork();
      my $command_data = shift(@commands);
      if($pid) {
        # Parent
        $children{$pid} = 1;
      } else {
        # Child
        my $error = 0;
        my @warnings;
        my $rows = 0;
        my $start = get_current_timestamp();
        my $t0 = [gettimeofday];

        my $dbh;
        eval {
          # Execute the query
          $::PL->i("Executing [".$command_data->{'command'}."] from file [".$command_data->{'filename'}."]") unless $quiet;
          $dbh = $dsn->get_dbh(1);
          $rows = $dbh->do($command_data->{'command'});
          $rows = 0 if $rows eq '0E0';
        };
        my $status = $@;

        my $t1 = [gettimeofday];
        my $elapsed = tv_interval($t0, $t1);
        my $end = get_current_timestamp();

        if($status) {
          my $errstr = defined($dbh) && $dbh->errstr() ? $dbh->errstr() : $status;
          $::PL->e("Command [".$command_data->{'command'}."] returned error [".$errstr."]");
          $error = 1;
        }

        my $sth = $dbh->prepare('SHOW WARNINGS');
        $sth->execute();
        while(my $warning = $sth->fetchrow_hashref) {
          push(@warnings, $warning);
        }

        # Our results
        my @out = ($command_data->{'filename'},
                   $rows,
                   scalar(@warnings) > 0 ? scalar(@warnings) : 'NULL',
                   $error ? $error : 'NULL',
                   $elapsed, $start, $end);

        flock(OUTPUTFILE, 2); # Exclusive lock
        print OUTPUTFILE join('|', @out)."\n";
        flock(OUTPUTFILE, 8); # Unlock

        exit($error == 1 ? -1 : 0); # Child exits
      }
    }

    if(scalar(keys %children) > 0) {
      # See if any children have finished.
      my $stiff = waitpid(-1, &WNOHANG);
      if($stiff == 0) {
        # Nothing finished yet, take a break
        sleep(2);
      } else {
        my $exit_value = $? >> 8;
        if($exit_value == 0) {
          # Finished without problems
        } else {
          if($abort_on_error) {
            # Wait for the children in progress and exit without spawning new ones
            $::PL->e("### Caught an error. Will wait for in-flight processes to finish and then will abort processing");
            $drain_and_exit = 1;
          }
        }
        $::PL->i("Process [".$stiff."] finished with exit code [".$exit_value."]") unless $quiet;
        delete $children{$stiff};
      }
    } elsif($drain_and_exit or scalar(@commands) == 0) {
      # We are done
      $done = 1;
    }
  }

  close(OUTPUTFILE);
  $::PL->i("Wrote results to $output_filename");
}

if(!caller) { exit(main(@ARGV)); }
1;

__END__

=head1 NAME

pdb-tasks-tracker - run tasks and compile information about them

=head1 RISKS AND BUGS

All software has bugs. This software is no exception. Care has been taken to ensure the safety of your data, however, no guarantees can be made. You are strongly advised to test run on a staging area before using this in production.

At the time of this writing, this software could use substantially more argument error checking. The program SHOULD simply die if incorrect arguments are passed, but, it could also delete unintended things. You have been warned.

=head1 SYNOPSIS

pdb-tasks-tracker [-h]

Run with -h or --help for options.

=head1 OPTIONS

=over 8

=item --help

This help.

Help is most awesome. It's like soap for your brain.

=item --condition

File to read commands from. This script will sit in a sleep loop until this file become available.

=item --dsn

DSN of MySQL server to perform commands against.

=item --abort-on-error

Processing will immediately stop if an error is encountered.

=item --quiet

Suppress some of the output

=item --logfile

File to log the output to

=back


