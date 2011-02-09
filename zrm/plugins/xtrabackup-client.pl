#!/usr/bin/perl

# This is the plugin to use to copy files from a remote machine
# that has the corresponding client installed.
# This uses sockets to copy from/to a remote machine.
#
# If the first command line parameter is --mysqlhotcopy then the plug will
# execute mysqlhotcopy on the remote machine and then copy the data over.
# Else it will just copy the requested data either from or to the given machine.
#
# If the first parameter to the plugin is --mysqlhotcopy then
# remaining Command line parameters that the plugin expects are
# [--host=<name>]
# [--user=<mysql username>]
# [--password=<password>"]
# [--port=<#>]
# [--socket=<socket file>]
# [--quiet]
# db_name[./table_regex/]
# directory
# Else the command line parameters that this plugin exects are
# --source-host <name>,
# --source-file <filename>,
# --destination-host <name>,
# --destination-directory <destination file>
################################################################################
use strict;
use warnings FATAL => 'all';

# ###########################################################################
# ProcessLog package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessLog package
# ###########################################################################

# ###########################################################################
# Which package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End Which package
# ###########################################################################

# ###########################################################################
# IniFile package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End IniFile package
# ###########################################################################

# ###########################################################################
# ZRMBackup package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End ZRMBackup package
# ###########################################################################

package XtraBackupClient;

use strict;
use warnings FATAL => 'all';
use Socket;
use File::Temp qw/ :POSIX /;
use Getopt::Long qw(:config no_ignore_case);
use File::Basename;
use File::Spec::Functions;
use Data::Dumper;
use Text::ParseWords;
use ProcessLog;
use IniFile;
use Which;
{
  no warnings 'once';
  $Data::Dumper::Indent = 0;
  $Data::Dumper::Sortkeys = 1;
}

my $TAR = "tar";
my $TAR_WRITE_OPTIONS = "";
my $TAR_READ_OPTIONS = "";

my $VERSION = "0.75.1";
my $REMOTE_PORT=25300;

my %o;
my %c;

$SIG{'PIPE'} = sub { $::PL->end; die "Pipe broke"; };
$SIG{'TERM'} = sub { close SOCK; $::PL->end; die "TERM broke\n"; };

$SIG{'__DIE__'} = sub { die(@_) if($^S); $::PL->e(@_); $::PL->end; exit(1); };

# This validates all incoming data, to ensure it's sane.
# This will only allow and a-z A-Z 0-9 _ - / . = " ' ; + * and space.
sub checkIfTainted {
  if( $_[0] =~ /^([-\*\w\/"\'.\@\:;\+\s=\^\$]+)$/) {
    return $1;
  }
  else {
    printAndDie("Bad data in $_[0]\n");
  }
}

# Reads a key=value block from the incoming stream.
# The format of a key=value block is as follows:
# <number of lines(N) to follow>\n
# <key=value\n>{N}
#
# N, is allowed to be 0.
# This function returns a hashref of the read key=value pairs.
#
sub readKvBlock {
  my $fh = shift;
  my %kv = ();
  my ($i, $N) = ((), 0, 0);
  chomp($N = <$fh>);
  checkIfTainted($N);
  if($N !~ /^\d+$/) {
    printAndDie("Bad input: $_");
  }
  for($i = 0; $i < $N; $i++) {
    chomp($_ = <$fh>);
    checkIfTainted($_);
    my ($k, $v) = split(/=/, $_, 2);
    $v = undef if($v eq '');
    $kv{$k} = $v;
  }
  $_ = <$fh>;
  return \%kv;
}

# Given a realhash, this returns a string in the format:
# <N>\n
# <key>=<value>\n{N}
#
# Where 'N' is the number of keys in the hash.
#
sub makeKvBlock {
  my %Kv = @_;
  $::PL->d('makeKvBlock:', Dumper(\%Kv));
  my $out = scalar(keys %Kv). "\n";
  foreach my $k (keys %Kv) {
    $out .= "$k=". (defined $Kv{$k} ? $Kv{$k} : '') . "\n";
  }
  $out .= "\n";
  $::PL->d('KvBlock:', $out);
  return $out;
}

sub agentWrite {
  my (%Args) = @_;
  print(SOCK makeKvBlock(%Args));
}

sub agentRead {
  return readKvBlock(\*SOCK);
}

sub agentRequest {
  my ($host, $port, $action, $sid, %params) = @_;
  $::PL->d('Attempting to connect to:', $host, 'port:', $port, "\n",
           "action:", $action, "sid:", $sid);
  my $iaddr = inet_aton($host) or die "no host: $host";
  my $paddr = sockaddr_in($port, $iaddr);
  my $proto = getprotobyname('tcp');
  socket(SOCK, PF_INET, SOCK_STREAM, $proto) or die "socket: $!";
  connect(SOCK, $paddr) or die "connect: $!";
  select( SOCK );
  $| = 1;
  select( STDOUT );
  $::PL->m("Connected to $host:$port.");

  my $tmp = File::Spec->tmpdir();
  my $args = makeKvBlock('action' => $action, 'tmpdir' => $tmp,
                          'sid' => $sid, %c, %params);
  $::PL->d("Sending initial agent parameters:\n", $args);
  print SOCK "$VERSION\n";
  print SOCK $args;
  $_ = <SOCK>;
  if(!/READY/) {
    printAndDie("Agent did not respond properly. Expected: READY, Got: $_");
  }
}

sub printAndDie {
  $::PL->ed(@_);
  $::PL->end;
  die("ERROR: @_\n");
}

sub my_exit {
  $::PL->end;
  exit($_[0]);
}

# This will read the data from the socket and pipe the output to tar
sub readTarStream {
  my $tmpfile = tmpnam();
  my $destDir = $o{'destination-directory'};
  my $tar_cmd = "|$TAR $TAR_READ_OPTIONS $destDir 2>$tmpfile";
  $::PL->m("read-tar-stream:\n$tar_cmd\n");
  unless( open( TAR_H, "$tar_cmd" ) ){
    printAndDie("tar failed $!");
  }
  binmode( TAR_H );

  my $buf;

  # Initially read the length of data to read
  # This will be packed in network order
  # Then read that much data which is uuencoded
  # Then write the unpacked data to tar
  while( read( SOCK, $buf, 4 ) ){
    $buf = unpack( "N", $buf );
    if($buf > 8*1024*1024) {
      # Buffer should never be larger than this.
      # So, we abort if it is.
      # This handles the case where the other side dies
      # and garbage is sent.
      last;
    }
    read SOCK, $buf, $buf;

    print TAR_H unpack( "u", $buf );
  }
  {
    local $/;
    open my $fh, "<$tmpfile";
    my $errs = <$fh>;
    chomp($errs);
    $::PL->e("tar-errors:", $errs);# if($errs !~ /\s*/);
    close $fh;
    unlink $tmpfile;
  }
  unless( close(TAR_H) ){
    printAndDie('tar pipe failed');
  }
}

# This just fiddles with the tar options to get it to read an innobackupex
# compatible tar stream, and optionally runs --apply-log, if requested by
# the xtrabackup-client:run-apply-log=1 option.
sub readInnoBackupStream {
  if( $c{'xtrabackup-client:tar-force-ownership'} == 0 ) {
    $TAR_READ_OPTIONS = "--no-same-owner --no-same-permissions -xiC";
  }
  else {
    $TAR_READ_OPTIONS = "--same-owner -xipC";
  }

  readTarStream();

  if( $c{'backup-level'} == 0 and $c{'xtrabackup-client:run-apply-log'} == 1 ) {
    if(not defined $c{'xtrabackup-client:innobackupex-path'}) {
      $::PL->i("Unable to determine path to innobackupex-1.5.1.\n",
      'Check your PATH, or set xtrabackup-client:innobackupex-path in your config.');
    }
    else {
      $::PL->m("Applying logs..");
      my $r = $::PL->x(sub { system @_; },
          "cd $o{'destination-directory'} && ".
          "$c{'xtrabackup-client:innobackupex-path'} ".
          "--apply-log $o{'destination-directory'}"
          );
      my $fh = $$r{fh};
      while(<$fh>) { $::PL->m($_); }
      if($$r{rcode} != 0) {
        $::PL->e("Applying the innobackup logs failed.");
      }
      if($$r{error}) { printAndDie("Error executing innobackupex."); }
    }
  }
}

sub dummySnapshotDeviceInfo {
  my %snap_o;
  ## This annoying paradigm brought to you by ZRM.
  ## It does a slight of hand when dealing with "snapshot plugins",
  ## When the snapshot plugin is called on the backup server machine, the
  ## module ZRM::SnapshotCommon detects this and transparently turns it into
  ## a call to the copy plugin and passes the arguments destined for the
  ## snapshot plugin on the remote end as a single string to the flag
  ## '--snapshot-parameters'. Since I don't feel like doing a regex just to
  ## pull out the '--action' flag, I save and restore @ARGV and call
  ## GetOptions().
  ##
  ## If the action is 'get-vm-device-details' then just *print* a dummy
  ## block of text for use to ZRM. ZRM catches this information and normally
  ## does some innane thing to it, since we don't actually *make* a snapshot,
  ## we can return any old thing - the values here are selected to be as
  ## impossible as can be.
  ##
  ## All other actions are ignored.

  my @ARGS_SAVE = @ARGV;
  @ARGV = shellwords($o{'snapshot-parameters'});
  GetOptions(\%snap_o,
             'dev=s',
             'action=s',
             'directory=s',
             'fstype=s',
             'sname=s',
             'size=s',
           );
  @ARGV = @ARGS_SAVE;
  $::PL->d('Snapshot action:', $snap_o{'action'});
  if($snap_o{'action'} eq 'get-vm-device-details') {
    print(STDOUT "device=/dev/null\n");
    print(STDOUT "snapshot-device=/dev/null\n");
    print(STDOUT "device-mount-point=null\n");
    print(STDOUT "filesystem-type=null\n");
    print(STDOUT "relative-copy-dir=0;0;0\n");
    print(STDOUT "snapshot-mount-point=/tmp\n");
  }
  return 0;
}

sub main {
  @ARGV = @_;
  %o = ( 'backupset-config' => $ENV{ZRM_CONF} );
  %c = ();

  GetOptions(\%o,
             'host=s',
             'user=s',
             'password=s',
             'port=s',
             'quiet',
             'backupset-config=s',
             'backup-dir=s',
             'type-of-dir=s',

             'mysqlhotcopy',
             'create-link',
             'source-host=s',
             'source-file=s',
             'destination-host=s',
             'destination-directory=s',

             'snapshot-parameters=s',
             'snapshot-command',
             'sname=s',
           );

  ## Load the config file given to us by ZRM.
  unless( $o{'backupset-config'} and
            %c = IniFile::read_config($o{'backupset-config'}) ) {
    die "Unable to open the config provided by ZRM or a test harness.\n";
  }
  ## ZRM uses INI-like configs that don't have groups. IniFile module
  ## treats global options as belonging to the empty-name group.
  %c = %{$c{''}};

  ## Setup our default values.
  $c{'xtrabackup-agent:port'} ||= $REMOTE_PORT;
  $c{'xtrabackup-client:logpath'} ||= '/var/log/mysql-zrm/xtrabackup-client.log';
  $c{'xtrabackup-client:email'}   ||= '';
  if( not exists $c{'xtrabackup-client:tar-force-ownership'} ) {
    $c{'xtrabackup-client:tar-force-ownership'} = 1;
  }
  if( not exists $c{'xtrabackup-client:run-apply-log'} ) {
    $c{'xtrabackup-client:run-apply-log'} = 0;
  }

  if( not exists $c{'xtrabackup-client:innobackupex-path'} ) {
    $c{'xtrabackup-client:innobackupex-path'} = Which::which('innobackupex-1.5.1');
  }

  $::PL->logpath($c{'xtrabackup-client:logpath'});
  $::PL->quiet($c{'verbose'});
  $::PL->start;

  $::PL->d('arguments:', Dumper(\%o));
  $::PL->d('configuration:', Dumper(\%c));

  if($^O eq "linux") {
    $TAR_WRITE_OPTIONS = "--same-owner -cphsC";
    $TAR_READ_OPTIONS = "--same-owner -xphsC";
  }
  elsif($^O eq "freebsd") {
    $TAR_WRITE_OPTIONS = " -cph -f - -C";
    $TAR_READ_OPTIONS = " -xp -f - -C";
  }
  else {
    printAndDie("Unable to determine which tar options to use!");
  }

  if( $c{"xtrabackup-client:tar-force-ownership"} == 0
        or $c{"xtrabackup-client:tar-force-ownership"} =~ /[Nn][oO]?/ ) {
    $c{"xtrabackup-client:tar-force-ownership"} = 0;
    if( $^O eq "linux" ) {
      $TAR_WRITE_OPTIONS = "--no-same-owner --no-same-permissions -chsC";
      $TAR_READ_OPTIONS = "--no-same-owner --no-same-permissions -xhsC";
    }
    elsif( $^O eq "freebsd" ) {
      $TAR_WRITE_OPTIONS = " -ch -f - -C";
      $TAR_READ_OPTIONS  = " -x -f - -C";
    }
  }

  if($o{'source-host'} and $o{'source-file'}
      and $o{'destination-host'} and $o{'destination-directory'}) {

    ## ZRM Calls this script multiple times in the case of incremental backups
    ## The SID (session id, roughly) allows us and the agent to identify when
    ## have been called before and to relate otherwise unrelated requests.
    ## We use the destination directory as the SID because it's virtually
    ## guaranteed to be unique. The only way it couldn't was if two backups
    ## for the same backupset were done in the same second. Highly unlikely.
    my $sid = $o{'destination-directory'};
    my $prev_backup = undef;
    my $next_binlog;

    if($o{'destination-directory'}) {
      $::PL->d('Attempting to open previous backup index to fetch binary log information.');
      ## This ridiculous dance is done because ZRM doesn't write out any amount
      ## of sensible to the index until after it's done with the backup, nor
      ## does it allow us to write to the index since it overwrites the index wholesale.
      ## Finally, we can't put anything in the backup directory that we want later,
      ## it'll be treated as backup data and deleted.
      eval {
        my $fh;
        my $last_dir;
        open($fh, "</etc/mysql-zrm/".
            ZRMBackup->new($o{'destination-directory'})->backup_set().
            "/last_backup"
            );
        chomp($last_dir = <$fh>);
        close($fh);
        $prev_backup = ZRMBackup->new($last_dir);
      };
      if($@) {
        $::PL->i("(Probably harmless): Unable to open last backup: $@");
      }
    }

    if($prev_backup and $prev_backup->next_binlog()) {
      $next_binlog = $prev_backup->next_binlog();
      $::PL->d('Found previous incremental backup, next binlog:', $next_binlog);
    }

    agentRequest($o{'source-host'}, $c{'xtrabackup-agent:port'},
      'copy from', $sid, 'file' => $o{'source-file'}, 'binlog' => $next_binlog);

    if($c{'backup-level'} == 0) {
      $::PL->m('Writing full backup to', $o{'destination-directory'});
      readInnoBackupStream();
    }
    elsif($c{'backup-level'} == 1) {
      if($c{'replication'} == 0) {
        $::PL->i('With replication=0, you cannot make a new secondary master.');
      }

      my $slave_status = agentRead();

      ## Normally ZRM calls this script once for every binlog it wishes
      ## to have copied, however, this is needlessly inefficient.
      ## The first time we're called with a binlog parameter, we copy
      ## ALL the binlogs and then the server ignores further requests for binlogs
      ## with the same SID.
      ## When it ignores a request it sends 'status=OK', as the slave information.
      if($slave_status->{status} eq "SENDING") {
        delete $slave_status->{status};
        my $fh;
        open($fh, ">$o{'destination-directory'}/master.info");
        print($fh join("\n",
              map {
                "$_=". (defined $$slave_status{$_} ? $$slave_status{$_} : 'NULL')
              } sort keys %$slave_status));
        close($fh);
        readTarStream();
      }
    }
  }
  elsif($o{'snapshot-command'}) {
    $::PL->d('Trapped snapshot command, doing dummy actions.');
    dummySnapshotDeviceInfo();
  }

  close( SOCK );
  select( undef, undef, undef, 0.250 );

  my_exit(0);

}

if(!caller) { exit(main(@ARGV)); }

1;
