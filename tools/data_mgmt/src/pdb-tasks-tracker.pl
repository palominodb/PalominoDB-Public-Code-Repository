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

package pdb_archiver;
use strict;
use warnings;
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
my $condition_filename;
my $output_filename;
my $abort_on_error = 0;

my %results;
my $email = undef;

sub main {
  my @ARGV = @_;
  GetOptions(
    'help' => sub { pod2usage(); },
    'condition=s' => \$condition_filename,
    'dsn=s' => \$dsn,
    'abort-on-error' => \$abort_on_error
  );

  my $PL = $::PL ? $::PL : ProcessLog->new($0, '/dev/null');

  unless($condition_filename) {
    pod2usage("CONDITION required");
  }

  unless($dsn) {
    pod2usage("DSN required");
  }

  eval {
    $dsn          = DSNParser->default()->parse($dsn);
    # Do we have mandatory fields? FIXME
  };
  if($@) {
    pod2usage($@);
  }

  my $dbh = $dsn->get_dbh(1);

  $PL->i("Will write results to ".$condition_filename.".result");
  $PL->i("Waiting for conditional file ".$condition_filename);
  while(! -f $condition_filename) {
    $PL->i(".");
    sleep(2);
  }
  $output_filename = $condition_filename.'.output';

  my @commands;
  open(FILE, "<$condition_filename") or die("Couldn't open $condition_filename: $!\n");
  while(my $line = <FILE>) {
    # FIXME: We need some sample inputs to know whether anything should be filtered out at this point.
    chomp($line);
    next if $line =~ /^\s*$/;
    push(@commands, $line);
  }
  close(FILE);

  $PL->i("Read ".scalar(@commands)." commands from file $condition_filename");

  # Open the output file before we start processing, just to make sure we won't fail
  # after doing all the work.
  open(OUTPUTFILE, ">$output_filename") or die("Couldn't open $output_filename");

  my $total_rows = 0;
  my @all_warnings;
  my $t_started = [gettimeofday];
  foreach my $command (@commands) {
    my %data;
    $data{'command'} = $command;

    my $rows = 0;
    my $t0 = [gettimeofday];
    eval {
      $rows = $dbh->do($command);
    };
    my $t1 = [gettimeofday];

    if($@) {
      $PL->e("Command [".$command."] returned error [".$dbh->errstr()."]");
      $data{'status'} = 'FAILURE';
    } else {
        $data{'status'} = 'SUCCESS';
    }

    $data{'warnings'} = [];
    my $sth = $dbh->prepare('SHOW WARNINGS');
    $sth->execute();
    while(my $warning = $sth->fetchrow_hashref) {
      push(@{$data{'warnings'}}, $warning);
      push(@{$results{'warnings'}}, $warning);
    }

    $data{'rows'} = $rows;
    $total_rows += $rows;
    $data{'runtime'}  = tv_interval($t0, $t1);

    push(@{$results{'commands'}}, \%data);
    if($abort_on_error && $data{'status'} eq 'FAILURE') {
      $PL->e("Aborting processing");
      last;
    }
  }

  my $t_finished = [gettimeofday];
  my $t_elapsed = tv_interval($t_started, $t_finished);
  $results{'runtime'} = $t_elapsed;
  $results{'rows'} = $total_rows;

  print OUTPUTFILE Dumper(\%results)."\n";
  close(OUTPUTFILE);
  $PL->i("Wrote results to $output_filename");
}

exit(main(@ARGV));
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

=back


