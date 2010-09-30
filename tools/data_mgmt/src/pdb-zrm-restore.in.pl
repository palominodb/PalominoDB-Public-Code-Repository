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
# ZRMBackup package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End ZRMBackup package
# ###########################################################################

# ###########################################################################
# MysqlBinlogParser package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End MysqlBinlogParser package
# ###########################################################################

# ###########################################################################
# IniFile package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End IniFile package
# ###########################################################################

# ###########################################################################
# Path package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End Path package
# ###########################################################################

# ###########################################################################
# Which package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End Which package
# ###########################################################################

package pdb_zrm_restore;
use strict;
use warnings;
use Getopt::Long qw(:config no_ignore_case);
use Data::Dumper;
use Pod::Usage;
use DBI;
use File::Spec qw(splitdir);
use File::Path;
use File::Basename;
use Sys::Hostname;

use IniFile;
use ProcessLog;
use ZRMBackup;
use Path;
use Which;

my $pl;

sub main {
  @ARGV = @_;
  my %o;
  $o{'mysql-user'} = 'root';
  $o{'mysql-password'} = '';
  $o{'log-file'} = '/dev/null';

  # Locate our various external programs.
  $o{'mysqlbinlog'} = Which::which('mysqlbinlog');
  $o{'mysql'} = Which::which('mysql');
  $o{'innobackupex'} = Which::which('innobackupex-1.5.1');
  $o{'mysqld'} = Which::which('mysqld_safe');

  my @backups = ();
  my %cfg;
  my $datadir;
  GetOptions(\%o,
    'help' => sub { pod2usage(-verbose => 99) },
    'dry-run',
    'log-file|L=s',
    'identify-dirs|i',
    'estimate|e',
    'defaults-file|F=s',
    'mysql-user=s',
    'mysql-password=s',
    'reslave|r',
    'mysqld=s',
    'mysql=s',
    'mysqlbinlog=s',
    'innobackupex=s',
    'slave-user=s',
    'slave-password=s',
    'master-host=s',
    'rel-base|b=s',
    'strip|p=s',
    'point-in-time|t=s',
    'create-dirs',
    'skip-extract'
  );

  $pl = ProcessLog->new($0, $o{'log-file'}, undef);
  if(not exists $o{'identify-dirs'} and exists $o{i}) {
    $o{'identify-dirs'} = $o{i};
  }
  if(not exists $o{'defaults-file'} and exists $o{F}) {
    $o{'defaults-file'} = $o{F};
  }
  if(not exists $o{'estimate'} and exists $o{e}) {
    $o{'estimate'} = $o{e};
  }
  if(not exists $o{'rel-base'} and exists $o{b}) {
    $o{'rel-base'} = $o{b};
  }
  if(not exists $o{'strip'} and exists $o{p}) {
    $o{'strip'} = $o{p};
  }
  if(not exists $o{'point-in-time'} and exists $o{t}) {
    $o{'point-in-time'} = $o{p};
  }
  if(!$o{'identify-dirs'} and !$o{'defaults-file'}) {
    $pl->e('Must have --defaults-file or --identify-dirs at a minimum. Try --help.');
    return 1;
  }

  # Ensure that --mysqld points to the 'safe' shell script.
  if(not exists $o{'identify-dirs'} and (!$o{'mysqld'} or $o{'mysqld'} !~ /safe/)) {
    $pl->e('You must provide a path to mysqld_safe, *not* the raw binary. Try --help.');
    return 1;
  }

  # Collect all the backup set information straight away.
  eval {
    my $backup = ZRMBackup->new($pl, $ARGV[0]);
    @backups = $backup->find_full($o{'strip'}, $o{'rel-base'});
  };
  if($@ and $@ =~ /No full backup/) {
    $pl->e("Unable to find full backup for this backup-set.");
    return 1;
  }
  elsif($@) {
    chomp($@);
    $pl->e("Could not find any backups: $@");
    return 1;
  }

  # If we're just identifying dirs, print them out.
  if($o{'identify-dirs'}) {
    foreach my $b (@backups) {
      print $b->backup_dir, "\n";
    }
  }
  return 0 if($o{'identify-dirs'});

  # We must be doing an actual restore.
  if(!$o{'defaults-file'}) {
    $pl->e("Must specify --defaults-file for restore.");
    return 1;
  }

  %cfg = read_config($o{'defaults-file'});
  $datadir = $cfg{'mysqld'}{'datadir'};

  if($o{'create-dirs'}) {
    eval {
      mkpath($datadir);
      mkpath(dirname($cfg{'mysqld'}{'log-bin'}));
    };
    if($@) {
      $pl->e("Unable to create all directories for $datadir.", $@);
      return 1;
    }
  }

  unless( -d $datadir ) {
    $pl->e("Datadir doesn't exist.");
    return 1;
  }

  unless( -w $datadir ) {
    $pl->e("Cannot write to the datadir. Are you the right user?");
    return 1;
  }

  # Prepare an estimate and wait for enter
  # if we're not doing a dry run and --estimate was given.
  if(!$o{'dry-run'} && $o{'estimate'}) {
    make_estimate(@backups);
    $_ = <STDIN>; # Wait for enter.
  }

  # Remove the datadir, just in case it was
  # being used like a scratch area.
  unless($o{'dry-run'}) {
    unless($o{'skip-extract'}) {
      $pl->m("Removing contents of $datadir.");
      Path::dir_empty($datadir);
      $pl->m("Removing contents of $cfg{'mysqld'}{'log-bin'}.");
      Path::dir_empty(dirname($cfg{'mysqld'}{'log-bin'}));
    }
    else {
      $pl->i("Skipping emptying $datadir due to --skip-extract");
    }
  }

  # Extract the backups
  if(extract_backups(\%o, $datadir, @backups)) {
    $pl->e("Bailing due to extraction errors.");
    return 1;
  }

  if( -f "$datadir/xtrabackup_logfile" ) {
    $pl->m("Applying xtrabackup log.");
    unless($o{'dry-run'}) {
      my %r = %{$pl->x(sub { system(@_) }, "cd $datadir ; $o{'innobackupex'} --defaults-file=$o{'defaults-file'} --apply-log .")};
      if($r{rcode} != 0) {
        $pl->e("Error applying xtrabackup log:");
        $_ = $r{fh};
        while (<$_>) { $pl->e($_); }
        $pl->e("Bailing out.");
        return 1;
      }
    }
  }
  else {
    $pl->m("Target doesn't look like an xtrabackup, not attempting log apply.");
  }

  my $iblog_size = $cfg{'mysqld'}{'innodb_log_file_size'};
  if(defined $iblog_size) {
    # Convert to size in bytes
    if($iblog_size =~ /(\d+)[Mm]$/) {
      $iblog_size = $1*1024*1024;
    }
    elsif($iblog_size =~ /(\d+)[Gg]$/) {
      $iblog_size = $1*1024*1024*1024;
    }
    if(-s "$datadir/ib_logfile0" < $iblog_size or -s "$datadir/ib_logfile0" > $iblog_size) {
      $pl->i("ib_logfiles are not the size that $o{'defaults-file'} says they should be.");
      $pl->i("Removing the ib_logfiles.");
      unlink(<$datadir/ib_logfile*>);
    }
  }

  if($backups[-1]->backup_level == 1) {
    start_mysqld(\%o, \%cfg);

    # XXX Get binlog positions, and pipe into mysql command
    # XXX This trusts shell sorting.
    $pl->m("Applying binlogs.");
    unless($o{'dry-run'}) {
      $pl->m('Reading position information from', $datadir . '/xtrabackup_binlog_info');
      open BINLOG_INFO, '<', "$datadir/xtrabackup_binlog_info";
      my $l = <BINLOG_INFO>;
      close BINLOG_INFO;
      my ($binlog, $pos) = split(/\s+/, $l);
      my ($first_fname, $first_logno) = split( '\.', $binlog);
      my $binlog_pattern = $backups[-1]->incremental();
      my @logs = ();
      my $binlog_opts = '';
      for(sort(<$datadir/$binlog_pattern>)) {
        my ($fname, $logno) = split('\.', $_);
        if(int($first_logno) > int($logno)) {
          $pl->d('Skipping binlog:', $_);
          next;
        }
        if(int($first_logno) == int($logno)) {
          $pl->d('First binlog after backup point.');
          $binlog_opts = "--start-position=$pos";
        }

        push @logs, $_;
      }
      if($o{'point-in-time'}) {
        $pl->d("Adding --stop-datetime='". $o{'point-in-time'} ."' due to --point-in-time on commandline.");
        $binlog_opts .= " --stop-datetime='$o{'point-in-time'}'";
      }
      $pl->m('Applying:', @logs);
      $_ = join(' ', @logs);
      $pl->d("exec: $o{'mysqlbinlog'} $binlog_opts $_ | $o{'mysql'} --defaults-file=$o{'defaults-file'}");
      system("$o{'mysqlbinlog'} $binlog_opts $_ | $o{'mysql'} --defaults-file=$o{'defaults-file'}");
      if(($? >> 8) > 0) {
        stop_mysqld(\%o, \%cfg);
        $pl->e('Error applying binlog.');
        return 1;
      }
    }
    stop_mysqld(\%o, \%cfg);
    wait;
  }

  if($o{'dry-run'}) { make_estimate(@backups); }

  return 0;
}

if ( !caller ) { exit main(@ARGV); }

sub start_mysqld {
  my ($o,  $cfg) = @_;
  my %o = %$o;
  my %cfg = %$cfg;
  my $pid = fork;
  if($pid == 0) {

    $pl->i('attempting to chown', $cfg{'mysqld'}{'datadir'}, 'to',  "$cfg{'mysqld_safe'}{'user'}:$cfg{'mysqld_safe'}{'group'}");
    system("chown -R $cfg{'mysqld_safe'}{'user'}:$cfg{'mysqld_safe'}{'group'} $cfg{'mysqld'}{'datadir'}");

    if($cfg{'mysqld'}{'log-bin'}) {
      $pl->i('attempting to chown', dirname($cfg{'mysqld'}{'log-bin'}), 'to',  "$cfg{'mysqld_safe'}{'user'}:$cfg{'mysqld_safe'}{'group'}");
      system("chown -R $cfg{'mysqld_safe'}{'user'}:$cfg{'mysqld_safe'}{'group'} ". dirname($cfg{'mysqld'}{'log-bin'}));
    }

    my @path = File::Spec->splitdir($o{'mysqld'});
    pop @path; pop @path;
    my $mysqld_basedir = File::Spec->catdir(@path);
    $pl->i('mysqld basedir:', $mysqld_basedir);
    $pl->i('starting mysqld with:', $o{'mysqld'}, '--defaults-file='. $o{'defaults-file'}, '--skip-grant-tables', '--skip-networking');
    chdir $mysqld_basedir;
    unless($o{'dry-run'}) {
      exec "$o{'mysqld'} --defaults-file=$o{'defaults-file'} --skip-grant-tables --skip-networking"
    }
    else {
      exit(0);
    }
  }
  elsif(not defined $pid) {
    $pl->e('Unable to spawn mysqld:', $!);
    return undef;
  }
  else {
    # Makes sure mysqld has started completely before giving
    # control over to other code.
    unless($o{'dry-run'}) {
      while(read_pidfile($cfg{'mysqld'}{'pid-file'}) !~ /\d+/) { sleep 1; }
    }
    else { # This is so the log looks correctly ordered on --dry-run.
      sleep(1);
    }
  }

  return 0;
}

sub stop_mysqld {
  my ($o, $cfg) = @_;
  my %o = %$o;
  my %cfg = %$cfg;
  my $r = 0;
  $pl->i('killing mysqld with -15');
  unless($o{'dry-run'}) {
    $r = kill 15, read_pidfile($cfg{'mysqld'}{'pid-file'});
  }
  return $r;
}

sub make_estimate {
  my @backups = @_;
  my $kbytes = 0.0;
  foreach my $bk (@backups) {
    $kbytes += $bk->backup_level == 1 ? 5.0*$bk->backup_size : $bk->backup_size;
  }
  $pl->i("Space estimate (MB):", $kbytes/1024.0);
  return 0;
}

sub read_pidfile {
  my $pidfile = shift;
  my $pid;
  open my $fh, "<$pidfile" or return "";
  chomp($pid = <$fh>);
  close($fh);
  return $pid;
}

sub extract_backups {
  my ($o, $ddir, @backups) = @_;
  my %o = %$o;
  if($o{'skip-extract'}) {
    $pl->i("Skipping backup extraction due to --skip-extract");
    return 0;
  }
  $pl->m("Extracting backups to $ddir");
  my ($r, $fh) = (0, undef);
  foreach my $bk (@backups) {
    $pl->m("Extracting", $bk->backup_dir);
    unless( $o{'dry-run'} ) {
      ($r, $fh) = $bk->extract_to($ddir);
      if($r != 0) {
        $pl->e("Extraction errors:");
        while(<$fh>) { $pl->e($_); }
      }
      close($fh);
    }
  }
  return $r;
}

# Loads a my.cnf into a hash.
# of the form:
# key: group
# val: { <option> => <value> }
# Strips spaces and newlines.
sub read_config {
  my $file = shift;
  my %cfg = IniFile::read_config($file);
  unless(%cfg) {
    $pl->e("Unable to open defaults file: $file. Error: $!");
  }
  unless($cfg{'mysqld'}{'pid-file'}) {
    if($cfg{'mysqld_safe'}{'pid-file'}) {
      $cfg{'mysqld'}{'pid-file'} = $cfg{'mysqld_safe'}{'pid-file'};
    }
    else {
      $cfg{'mysqld'}{'pid-file'} = $cfg{'mysqld'}{'datadir'} .'/'. hostname() . '.pid';
    }
  }
  return %cfg;
}

1;

__END__

=head1 NAME

pdb-zrm-restore - Do a point-in-time restore from a zrm backup.

=head1 RISKS

This section is here to inform you that this tool may have bugs.
In general, this tool should be safe, provided that you do not test
it out in production. At the time of this release, there are no known
bugs, but that does not mean there are none.

It's completely possible to shoot yourself in the foot at this time.
This tool does no checking whatsoever to make sure you don't overwrite
an active mysql datadir. It B<will> just blindly empty it and assume it's got control.

=head1 SYNOPSIS

pdb-zrm-restore --defaults-file /etc/my.cnf [last backup directory]

=head1 ARGUMENTS

The only non-option argument passed to pdb-zrm-restore is a path
to a zrm backup directory. If the directory points to an incremental
backup, pdb-zrm-restore will walk backwards till it finds a full backup.

=head1 OPTIONS

=over 4

=item --help,-h

This help.

=item --dry-run

Report on actions that would be taken, and print an estimate of how much disk
space will be needed for the restore.

=item --log-file,-L

Sets the logfile that should be written to.

Default: /dev/null

=item --identify-dirs,-i

Rather than restoring, simply list all directories
up to the most recent full.

=item --estimate,-e

Estimate the space required by the restore and wait for enter to be pressed.

=item --defaults-file,-F

Use mysqld options from this file. In particular, pdb-zrm-restore
needs this option to determine WHERE to restore.

=item --create-dirs

pdb-zrm restore will create the path specified by mysql.datadir
in found in L<--defaults-file>.

=item --skip-extract

Instead of doing the whole extraction cycle, just apply the xtrabackup log,
and replay binlogs identified by the backup sets. This prevents pdb-zrm-restore
from emptying the contents of the datadir. Mysql must still not be running.

=item --rel-base,-b

If you've copied the backup data from another host,
and are unable or uninterested in duplicating the same directory
structure as on the backup server. This option causes pdb-zrm-restore
to look for backups relative to this directory. See the below example.

  On backup server: /mysqlbackups/<backup-set>/<datestamp>
  Using --rel-base /srv: /srv/mysqlbackups/<backup-set>/<datestamp>

This flag is often needed since this tool automaticallly tracks back
to the most recent full backup from the backup provided on the commandline.

This flag is always applied AFTER L<--strip> to help you readjust
the lookup path for backups.

Default: (none)

=item --strip,-p

If the value looks like a number, then this flag strips N path components
off the front of the backup-set directories. See the below example.

  backup-set dir from index: /mysqlbackups/<backup-set>/<datestamp>
  Using --strip 1: /<backup-set>/<datestamp>

Otherwise, it's assumed to be a leading path (starting with '/') to be
stripped off. See below:

  backup-set dir from index: /some/deep/path/<backup-set>/<datestamp>
  Using --strip /some/deep/path: /<backup-set>/<datestamp>

This flag is always applied BEFORE L<--rel-base> so that you can
readjust the lookup path for backups to suit your needs.

Default: 0

=item --point-in-time,-t

Apply binlogs up to an exact date. If there isn't a binlog
entry for the specific time given, logs will be applied until
as close as possible, but not past that time.

The date given must be in the format: C<YYYY-MM-DD HH:mm:SS>
Quoting to protect the space from the shell is likely necessary.

=item --mysql-user,-u

User in the restored database that has privileges to setup slaving.

Default: root

=item --mysql-password,-p

Password for the mysql user that has privileges to setup slaving.

Default: (no password)

=item --reslave,-r

Automatically reslaves the restored server according to the contents
of C<master.info>. If binlogs are applied, then the position will be
used from those instead of from the original C<master.info>.

=item --slave-user

If C<master.info> is missing, or has invalid data, this option
will override or set the user to slave with.

Default: from C<master.info>

=item --slave-password

If C<master.info> is missing, or has invalid data, this option
will override or set the password to slave with.

Default: from C<master.info>

=item --master-host

If C<master.info> is missing, or has invalid data, this option
will override or set the master host to connect to.

Default: from C<master.info>

=item --mysqld

Use this mysqld binary to start up the server for binlog replay
and reslaving configuration. This B<must> be the path to mysqld_safe.

Default: `which safe_mysqld`

=item --innobackupex

Use this to specify the full path to innobackupex, if it not in your path.

Default: `which innobackupex-1.5.1`


=back
