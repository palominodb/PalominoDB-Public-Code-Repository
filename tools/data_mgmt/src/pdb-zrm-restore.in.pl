#!/usr/bin/env perl
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

package pdb_zrm_restore;
use strict;
use warnings;
use Getopt::Long qw(GetOptionsFromArray :config no_ignore_case);
use Data::Dumper;
use Pod::Usage;
use DBI;
use File::Path qw(remove_tree);
use File::Spec qw(splitdir);

use ProcessLog;
use ZRMBackup;

my $mysqlbinlog_path;
my $mysql_path;
my $innobackupex_path;
my $mysqld_path;

my $pl;

sub main {
  my @ARGV = @_;
  my %o;
  $o{'mysql-user'} = 'root';
  $o{'mysql-password'} = '';

  # Locate our various external programs.
  chomp($mysqlbinlog_path  = qx/which mysqlbinlog/);
  chomp($mysql_path        = qx/which mysql/);
  chomp($innobackupex_path = qx/which innobackupex-1.5.1/);
  chomp($mysqld_path       = qx/which mysqld_safe/);

  my @backups;
  my %cfg;
  my $datadir;
  GetOptionsFromArray(\@ARGV, \%o,
    'help',
    'dry-run',
    'identify-dirs|i',
    'estimate|e',
    'defaults-file|F=s',
    'mysql-user=s',
    'mysql-password=s',
    'reslave|r',
    'mysqld=s',
    'innobackupex=s',
    'slave-user=s',
    'slave-password=s',
    'master-host=s',
    'rel-base|b=s',
    'strip|p=i'
  );

  $pl = ProcessLog->new($0, "/dev/null", undef);
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
  if(!$o{'identify-dirs'} and !$o{'defaults-file'}) {
    pod2usage(-message => 'Must have --defaults-file or --identify-dirs at a minimum', -verbose => 99);
  }

  # Ensure that --mysqld points to the 'safe' shell script.
  if(!$o{'identify-dirs'} and !$o{'mysqld'} or $o{'mysqld'} !~ /safe/) {
    pod2usage(-message => 'You must provide a path to mysqld_safe. *Not* the raw binary.', -verbose => 99);
  }

  # Collect all the backup set information straight away.
  unshift @backups, ZRMBackup->new($pl, $ARGV[0]);
  while($backups[0] && $backups[0]->backup_level != 0) {
    my @path = File::Spec->splitdir($backups[0]->last_backup);
    my $path;
    if($o{'strip'}) {
      for(my $i=0; $i<$o{'strip'}; $i++) { shift @path; }
    }
    if($o{'rel-base'}) {
      unshift @path, $o{'rel-base'};
    }
    $path = File::Spec->catdir(@path);
    unshift @backups, ZRMBackup->new($pl, $path);
  }
  shift @backups unless($backups[0]); # Remove that pesky undef
  if(scalar @backups == 0) {
    $pl->e("No backup directories found.");
    return 1;
  }

  # If we're just identifying dirs, print them out.
  if($o{'identify-dirs'}) {
    foreach my $b (@backups) {
      print $b->backup_dir, "\n";
    }
  }
  $pl->e("Unable to find full backup for this backup-set.") unless($backups[0]->backup_level == 0);
  return 0 if($o{'identify-dirs'});

  # We must be doing an actual restore.
  if(!$o{'defaults-file'}) {
    $pl->e("Must specify --defaults-file for restore.");
    return 1;
  }

  %cfg = read_config($o{'defaults-file'});
  $datadir = $cfg{'mysqld'}{'datadir'};
  unless( -d $datadir ) {
    $pl->e("Datadir doesn't exist.");
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
  $pl->m("Removing contents of $datadir.");
  unless($o{'dry-run'}) {
    remove_tree($datadir, { keep_root => 1 });
  }

  # Extract the backups
  if(extract_backups(\%o, $datadir, @backups)) {
    $pl->e("Bailing due to extraction errors.");
    return 1;
  }

  $pl->m("Applying xtrabackup log.");
  unless($o{'dry-run'}) {
    my %r = %{$pl->x(sub { system(@_) }, "pushd $datadir ; innobackupex-1.5.1 --apply-log $datadir ; popd")};
    if($r{rcode} != 0) {
      $pl->e("Error applying xtrabackup log:");
      $_ = $r{fh};
      while (<$_>) { $pl->e($_); }
      $pl->e("Bailing out.");
      return 1;
    }
  }

  if($backups[-1]->backup_level == 1) {
    start_mysqld(\%o, \%cfg);

    # XXX Get binlog positions, and pipe into mysql command
    $pl->m("Applying binlogs.");
    unless($o{'dry-run'}) {
      system("mysqlbinlog5 $datadir/*-bin.[0-9]* | mysql --defaults-file=$o{'defaults-file'} --user=$o{'mysql-user'} ". ($o{'mysql-password'} eq "" ? "" : "--password=$o{'mysql-password'}"));
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
    my @path = File::Spec->splitdir($o{'mysqld'});
    pop @path; pop @path;
    my $mysqld_basedir = File::Spec->catdir(@path);
    $pl->i('mysqld basedir:', $mysqld_basedir);
    $pl->i('starting mysqld with:', $o{'mysqld'}, '--defaults-file', $o{'defaults-file'});
    chdir $mysqld_basedir;
    unless($o{'dry-run'}) {
      exec "$o{'mysqld'} --defaults-file=$o{'defaults-file'}"
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

sub rm_datadir {
  my (%o, $ddir) = @_;
  return 0;
}

sub extract_backups {
  my ($o, $ddir, @backups) = @_;
  my %o = %$o;
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
  my %cfg;
  my $inif;
  unless(open $inif, "<$file") {
    $pl->e("Unable to open defaults file: $file");
    return undef;
  }
  my $cur_sec = '';
  while(<$inif>) {
    chomp;
    next if(/^\s*(?:;|#)/);
    next if(/^$/);
    if(/^\s*\[(\w+)\]/) {
      $cfg{$1} = ();
      $cur_sec = $1;
    }
    else {
      my ($k, $v) = split(/=/, $_, 2);
      $k =~ s/\s+$//;
      $v =~ s/^\s+//;
      chomp($k); chomp($v);
      $cfg{$cur_sec}{$k} = $v;
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

=item --identify-dirs,-i

Rather than restoring, simply list all directories
up to the most recent full.

=item --estimate,-e

Estimate the space required by the restore and wait for enter to be pressed.

=item --defaults-file,-F

Use mysqld options from this file. In particular, pdb-zrm-restore
needs this option to determine WHERE to restore.

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

This flag strips N path components off the front of the backup-set
directories. See the below example.

  backup-set dir from index: /mysqlbackups/<backup-set>/<datestamp>
  Using --strip 1: /<backup-set>/<datestamp>

This flag is always applied BEFORE L<--rel-base> so that you can
readjust the lookup path for backups to suit your needs.

Default: 0

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
