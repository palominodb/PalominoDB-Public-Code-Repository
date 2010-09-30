#!/usr/bin/perl

# This is meant to be invoked by xinetd.
# It expects two arguments on stdin
# First argument is the action to be taken.
# Valid actions are 'mysqlhotcopy', 'copy to', 'copy from'
# Second argument is the parameter list if mysqlhotcopy is the action specified
# or the file that needs to be copied if the action is copy.
# It will output the data on stdout after being uuencoded
# so that we only transfer ascii data.
# Each data block is preceeded by the size of the block being written.
# This date is encoded in Network order
# Remember that the communication is not secure and that this can be used to
# copy arbitary data from the host.
# Log messages can be found in /var/log/mysql-zrm/socket-server.log
# on the MySQL server

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

package main;
use strict;
use warnings FATAL => 'all';
use File::Path;
use File::Basename;
use File::Temp qw(:POSIX);
use IO::Select;
use IO::Handle;
use Sys::Hostname;
use ProcessLog;
use Which;
use POSIX;
use Tie::File;


# Set remote-mysql-binpath in mysql-zrm.conf if mysql client binaries are
# in a different location
my $MYSQL_BINPATH = "/usr/bin";

# File pointed to here is expected to contain the alternate path
# where plugins are installed.
# If file is not found the path /usr/share/mysql-zrm/plugins is used
my $SNAPSHOT_INSTALL_CONF_FILE = "/etc/mysql-zrm/plugin-path";

delete @ENV{'IFS', 'CDPATH', 'ENV', 'BASH_ENV'};
$ENV{PATH}="/usr/local/bin:/opt/csw/bin:/usr/bin:/usr/sbin:/bin:/sbin";
my $TAR = "tar";
my $TAR_WRITE_OPTIONS = "";
my $TAR_READ_OPTIONS = "";
my $LS = "ls";

my $TMPDIR;
my $tmp_directory;
my $action;
my $params;

my $INNOBACKUPEX="innobackupex-1.5.1";

my $VERSION="1.8b7_palomino";
my $MIN_XTRA_VERSION=1.0;

my $logDir = "/var/log/mysql-zrm";
my $logFile = "$logDir/socket-server.log";
my $snapshotInstallPath = "/usr/share/mysql-zrm/plugins";

# Set to 1 inside the SIGPIPE handler so that we can cleanup innobackupex gracefully.
my $stop_copy = 0;
$SIG{'PIPE'} = sub { &printLog( "caught broken pipe\n" ); $stop_copy = 1; };
$SIG{'TERM'} = sub { &printLog( "caught SIGTERM\n" ); $stop_copy = 1; };


my $stats_db       = "$logDir/stats.db";
my $stats_history_len = 100;
my $nagios_service = "MySQL Backups";
my $nagios_host = "nagios.example.com";
my $nsca_client = "/usr/sbin/send_nsca";
my $nsca_cfg = "/usr/share/mysql-zrm/plugins/zrm_nsca.cfg";
my $wait_timeout = 8*3600; # 8 Hours
my $must_set_wait_timeout = 0;
my $mycnf_path = "/etc/my.cnf";
my $mysql_socket_path = undef;
my $innobackupex_opts = "";

my ($mysql_user, $mysql_pass);

if( -f "/usr/share/mysql-zrm/plugins/socket-server.conf" ) {
  open CFG, "< /usr/share/mysql-zrm/plugins/socket-server.conf";
  while(<CFG>) {
    my ($var, $val) = split /\s+/, $_, 2;
    chomp($val);
    $var = lc($var);
    if($var eq "nagios_service") {
      $nagios_service = $val;
    }
    elsif($var eq "nagios_host") {
      $nagios_host = $val;
    }
    elsif($var eq "nsca_client") {
      $nsca_client = $val;
    }
    elsif($var eq "nsca_cfg") {
      $nsca_cfg = $val;
    }
    elsif($var eq "innobackupex_path") {
      $INNOBACKUPEX=$val;
    }
    elsif($var eq "mysql_wait_timeout") {
      # If mysql_wait_timeout is less than 3600, we assume
      # that the user specified hours, otherwise, we assume
      # that it is specified in seconds.
      # You'll note that there is no way to specify minutes.
      if(int($val) < 3600) { # 1 hour, in seconds
        $wait_timeout = int($val)*3600;
      }
      else {
        $wait_timeout = int($val);
      }
    }
    elsif($var eq "my.cnf_path") {
      $mycnf_path = $val;
    }
    elsif($var eq "mysql_install_path") {
      $ENV{PATH} = "$val:". $ENV{PATH};
    }
    elsif($var eq "perl_dbi_path") {
      eval "use lib '$val'";
    }
    elsif($var eq "mysql_socket") {
      $mysql_socket_path = $val;
    }
    elsif($var eq "innobackupex_opts") {
      $innobackupex_opts = $val;
    }
    elsif($var eq "must_set_wait_timeout") {
      $must_set_wait_timeout = $val;
    }
    elsif($var eq "stats_db") {
      $stats_db = $val;
    }
    elsif($var eq "stats_history_len") {
      $stats_history_len = $val;
    }
  }
}

my $PL = ProcessLog->new('socket-server', $logFile);
$PL->quiet(1);

if($^O eq "linux") {
  $TAR_WRITE_OPTIONS = "--same-owner -cphsC";
  $TAR_READ_OPTIONS = "--same-owner -xphsC";
}
elsif($^O eq "freebsd") {
  $TAR_WRITE_OPTIONS = " -cph -f - -C";
  $TAR_READ_OPTIONS = " -xp -f - -C";
}
else {
  #&printAndDie("Unable to determine which tar options to use!\n");
}

# This will only allow and a-z A-Z 0-9 _ - / . = " ' ; + * and space.
# Modify this if any other characters are to be allowed.
sub checkIfTainted(){
  if( $_[0] =~ /^([-\*\w\/"\'.\:;\+\s=\^\$]+)$/) {
    return $1;
  }else{
    &printAndDie("Bad data in $_[0]\n");
  }
}

sub my_exit(){
  if( $tmp_directory ){
    rmtree $tmp_directory, 0, 0;
  }
  exit( $_[0] );
}

sub printLog {
  my @args = @_;
  chomp(@args);
  $PL->m(@args);
}

sub printAndDie {
  my @args = @_;
  chomp(@args);
  $PL->e(@args);
  &my_exit( 1 );
}

sub getInputs()
{
  my @inp;
  my $x = <STDIN>;
  chomp( $x );
  $x = &checkIfTainted($x);
  if( $x ne $VERSION ){
    &printAndDie( "Version of remote copy plugin does not match\n" );
  }
  for( my $i = 0; $i < 4; $i++ ){
    $x = <STDIN>;
    push @inp, $x;
  }
  chomp( @inp );
  $action = &checkIfTainted($inp[0]);
  $params = &checkIfTainted($inp[1]);
  $TMPDIR = &checkIfTainted($inp[2]);
  $MYSQL_BINPATH = &checkIfTainted($inp[3]);
}

sub restore_wait_timeout {
  my ($dbh, $prev_wait) = @_;

  if($dbh and $prev_wait){
    &printLog("Re-setting wait_timeout to $prev_wait\n");
    $dbh->do("SET GLOBAL wait_timeout=$prev_wait");
  }
  else {
    undef;
  }
  undef;
}

sub doRealHotCopy()
{
  my ($start_tm, $backup_sz) = (time(), 0);
  record_backup("full", $start_tm);
  if($stop_copy == 1) {
    # It's possible we could be interrupted before ever getting here.
    # Catch this.
    return;
  }
  # massage params for innobackup
  $params =~ s/--quiet//;
  my $new_params = "";
  foreach my $i (split(/\s+/, $params)) {
    &printLog("param: $i\n");
    next if($i !~ /^--/);
    next if($i =~ /^--host/);
    $new_params .= "$i ";
  }

  my ($fhs, $buf);
  POSIX::mkfifo("/tmp/innobackupex-log", 0700);
  &printLog("Created FIFOS..\n");

  my $dbh = undef;
  my $prev_wait = undef;
  eval {
    require "DBI";
    $dbh = DBI->connect("DBI:mysql:host=localhost". $mysql_socket_path ? ";mysql_socket=$mysql_socket_path" : "", $mysql_user, $mysql_pass, { RaiseError => 1, AutoCommit => 1});
  };
  if( $@ ) {
    &printLog("Unable to open DBI handle. Error: $@\n");
    if($must_set_wait_timeout) {
      record_backup("full", $start_tm, time(), $backup_sz, "failure", "$@");
      &printAndDie("ERROR", "Unable to open DBI handle. $@\n");
    }
  }

  if($dbh) {
    $prev_wait = $dbh->selectrow_arrayref("SHOW GLOBAL VARIABLES LIKE 'wait_timeout'")->[1];
    eval {
      $dbh->do("SET GLOBAL wait_timeout=$wait_timeout");
    };
    if( $@ ) {
      &printLog("Unable to set wait_timeout. $@\n");
      if($must_set_wait_timeout) {
        record_backup("full", $start_tm, time(), $backup_sz, "failure", "unable to set wait_timeout");
        &printAndDie("ERROR", "Unable to set wait_timeout. $@\n");
      }
    }
    &printLog("Got db handle, set new wait_timeout=$wait_timeout, previous=$prev_wait\n");
  }


  open(INNO_TAR, "$INNOBACKUPEX $new_params --defaults-file=$mycnf_path $innobackupex_opts --slave-info --stream=tar $tmp_directory 2>/tmp/innobackupex-log|");
  &printLog("Opened InnoBackupEX.\n");
  open(INNO_LOG, "</tmp/innobackupex-log");
  &printLog("Opened Inno-Log.\n");
  $fhs = IO::Select->new();
  $fhs->add(\*INNO_TAR);
  $fhs->add(\*INNO_LOG);
  $SIG{'PIPE'} = sub { &printLog( "caught broken pipe\n" ); $stop_copy = 1; };
  $SIG{'TERM'} = sub { &printLog( "caught SIGTERM\n" ); $stop_copy = 1; };
  while( $fhs->count() > 0 ) {
    if($stop_copy == 1) {
      restore_wait_timeout($dbh, $prev_wait);
      &printLog("Copy aborted. Closing innobackupex.\n");
      $fhs->remove(\*INNO_TAR);
      $fhs->remove(\*INNO_LOG);
      close(INNO_TAR);
      close(INNO_LOG);
      &printLog("Copy aborted. Closed innobackupex.\n");
      sendNagiosAlert("WARNING: Copy was interrupted!", 1);
      unlink("/tmp/innobackupex-log");
      record_backup("full", $start_tm, time(), $backup_sz, "failure", "copy interrupted");
      &printAndDie("ERROR", "Finished cleaning up. Bailing out!\n");
    }
    my @r = $fhs->can_read(5);
    foreach my $fh (@r) {
      if($fh == \*INNO_LOG) {
        if( sysread( INNO_LOG, $buf, 10240 ) ) {
          &printLog($buf);
          if($buf =~ /innobackupex: Error:(.*)/ || $buf =~ /Pipe to mysql child process broken:(.*)/) {
            record_backup("full", $start_tm, time(), $backup_sz, "failure", $1);
            restore_wait_timeout($dbh, $prev_wait);
            sendNagiosAlert("CRITICAL: $1", 2);
            unlink("/tmp/innobackupex-log");
            &printAndDie($_);
          }
        }
        else {
          &printLog("closed log handle\n");
          $fhs->remove($fh);
          close(INNO_LOG);
        }
      }
      if($fh == \*INNO_TAR) {
        if( sysread( INNO_TAR, $buf, 10240 ) ) {
          $backup_sz += length($buf);
          my $x = pack( "u*", $buf );
          print STDOUT pack( "N", length( $x ) );
          print STDOUT $x;
        }
        else {
          &printLog("closed tar handle\n");
          $fhs->remove($fh);
          close(INNO_TAR);
          if($^O eq "freebsd") {
            &printLog("closed log handle\n");
            $fhs->remove(\*INNO_LOG);
            close(INNO_LOG);
          }
        }
      }
    }
  }
  unlink("/tmp/innobackupex-log");
  if($dbh) {
    restore_wait_timeout($dbh, $prev_wait);
    $dbh->disconnect;
  }
  record_backup("full", $start_tm, time(), $backup_sz, "success");
  sendNagiosAlert("OK: Copied data successfully.", 0);
}

sub sendNagiosAlert {
  my $alert = shift;
  my $status = shift;
  my $host = hostname;
  $status =~ s/'/\\'/g; # Single quotes are bad in this case.
  &printLog("Pinging nagios with: echo -e '$host\t$nagios_service\\t$status\\t$alert' | $nsca_client -c $nsca_cfg -H $nagios_host\n");
  $_ = qx/echo -e '$host\\t$nagios_service\\t$status\\t$alert' | $nsca_client -c $nsca_cfg -H $nagios_host/;
}

#$_[0] dirname
#$_[1] filename
sub writeTarStream()
{
  my ($start_tm, $backup_sz) = (time(), 0);
  my $fileList = $_[1];
  my $lsCmd = "";

  my $tmpFile = getTmpName();

  if( $_[1] =~ /\*/){
    $lsCmd = "cd $_[0]; $LS -1 $_[1] > $tmpFile 2>/dev/null;";
    my $r = system( $lsCmd );
    $fileList = " -T $tmpFile";
  }

  &printLog("writeTarStream: $TAR $TAR_WRITE_OPTIONS $_[0] $fileList\n");
  unless(open( TAR_H, "$TAR $TAR_WRITE_OPTIONS $_[0] $fileList 2>/dev/null|" ) ){
    record_backup("incremental", $start_tm, time(), $backup_sz, "failure", "$!");
    &printAndDie( "tar failed $!\n" );
  }
  binmode( TAR_H );
  my $buf;
  while( read( TAR_H, $buf, 10240 ) ){
    my $x = pack( "u*", $buf );
    $backup_sz += length($buf);
    print pack( "N", length( $x ) );
    print $x;
  }
  close( TAR_H );

  if( $lsCmd ){
    unlink( $tmpFile );
  }
  record_backup("incremental", $start_tm, time(), $backup_sz, "success", $fileList);
}

#$_[0] dirname to strea the data to
sub readTarStream()
{
  &printLog("readTarStream: $TAR $TAR_READ_OPTIONS $_[0]\n");
  unless(open( TAR_H, "|$TAR $TAR_READ_OPTIONS $_[0] 2>/dev/null" ) ){
    &printAndDie( "tar failed $!\n" );
  }

  my $buf;
  # Initially read the length of data to read
  # This will be packed in network order
  # Then read that much data which is uuencoded
  # Then write the unpacked data to tar
  while( read( STDIN, $buf, 4 ) ){
    $buf = unpack( "N", $buf );
    read STDIN, $buf, $buf;
    print TAR_H unpack( "u", $buf );
  }
  unless( close(TAR_H) ){
    &printAndDie( "close of pipe failed\n" );
  }
  close( TAR_H );
}

sub getTmpName()
{
  if( ! -d $TMPDIR ){
    &printAndDie( "$TMPDIR not found. Please create this first.\n" );
  }
  &printLog( "TMP directory being used is $TMPDIR\n" );
  return File::Temp::tempnam( $TMPDIR, "" );
}

sub validateSnapshotCommand()
{
  my $file = basename( $_[0] );
  if( -f $SNAPSHOT_INSTALL_CONF_FILE ){
    if( open( TMP, $SNAPSHOT_INSTALL_CONF_FILE ) ){
      $snapshotInstallPath = <TMP>;
      close TMP;
      chomp( $snapshotInstallPath );
    }
  }
  my $cmd = "$snapshotInstallPath/$file";
  if( -f $cmd ){
    return $cmd;
  }
  return "";
}

sub printToServer()
{
  my @data = @_;
  my $status = shift @data;
  my $cnt = @data;
  &printLog( "status=$status cnt=$cnt" , join("\n", @data));
  print "$status\n";
  print "$cnt\n";
  my $i;
  for( $i = 0; $i < $cnt; $i++ ){
    print "$data[$i]\n";
  }
}

#$_[0] name of file
sub printFileToServer()
{
  my @x = "";
  if( open( TMP, $_[1] ) ){
    @x = <TMP>;
    close TMP;
    chomp( @x );
    &printToServer( $_[0], @x );
  }
}

sub readOneLine()
{
  my $line = <STDIN>;
  chomp( $line );
  $line = &checkIfTainted( $line );
  return $line;
}

sub doSnapshotCommand()
{
  my $cmd = &readOneLine();
  &printLog('snapshot cmd:', $cmd);
  my $num = &readOneLine();

  my @confData;
  my $i;
  for( $i = 0; $i < $num;$i++ ){
    my $str = &readOneLine();
    push @confData, $str;
  }

  my $command = &validateSnapshotCommand( $cmd );
  if( $command eq "" ){
    &printToServer( "ERROR", "Snapshot Plugin $cmd not found" );
    &printAndDie( "Snapshot Plugin $cmd not found\n" );
  }
  my $file = tmpnam();
  $file = basename( $file );
  $file = "$tmp_directory/$file";
  if( ! open( TMP, ">$file" ) ){
    &printToServer( "ERROR", "Unable to open temp file $file" );
    &printAndDie( "Unable to open temp file $file\n" );
  }
  foreach( @confData ){
    print TMP "$_\n";
  }
  close TMP;
  $ENV{'ZRM_CONF'} = $file;
  my $f = tmpnam();
  my $ofile = basename($f);
  $f = tmpnam();
  my $efile = basename($f);
  $command .= " $params > $tmp_directory/$ofile 2>$tmp_directory/$efile";
  my $r = system( $command );
  if( $r == 0 ){
    &printFileToServer( "SUCCESS", "$tmp_directory/$ofile" );
  }else{
    &printFileToServer( "ERROR", "$tmp_directory/$efile" );
  }
}

sub doCopyBetween()
{
  $ENV{'TMPDIR'} = $TMPDIR;
  my $port = &readOneLine();
  my $f = tmpnam();
  $f = basename( $f );
  $f = "$tmp_directory/$f";
  unless( open( T, ">$f" ) ){
    &printToServer( "ERROR", "Unable to open temp file $f" );
    &printAndDie( "Unable to open temp file $f\n" );
  }
  print T "$port\n";
  close T;
  $ENV{'ZRM_CONF'}=$f;
  $f = tmpnam();
  my $ofile = basename($f);
  $f = tmpnam();
  my $efile = basename($f);
  my $cmd = "/usr/share/mysql-zrm/plugins/socket-copy.palomino.pl $params > $tmp_directory/$ofile 2>$tmp_directory/$efile";
  &printLog( "$cmd\n" );
  my $r = system( $cmd );
  if( $r == 0 ){
    &printFileToServer( "SUCCESS", "$tmp_directory/$ofile" );
  }else{
    &printFileToServer( "ERROR", "$tmp_directory/$efile" );
  }
}

sub record_backup {
  my ($type, $start_tm, $end_tm, $sz, $status, $info) = @_;
  my (@all_stats, $i, $upd) = ((), 0, 0);
  if(not defined $type or not defined $start_tm) {
    die("Programming error. record_backup() needs at least two parameters.");
  }
  $end_tm = '-' if(not defined $end_tm);
  $sz = '-' if(not defined $sz);
  $status = '-' if(not defined $status);
  $info = '-' if(not defined $info);


  tie @all_stats, 'Tie::File', $stats_db or &printAndDie("ERROR", "unable to open the stats database $stats_db");
  for($i = 0; $i < @all_stats; $i++) {
    my $stat = $all_stats[$i];
    my ($stype, $sstart, $send, $ssize, $sstatus, $sinfo) = split(/\t/, $stat);
    if($stype eq $type and $start_tm == $sstart) {
      $all_stats[$i] = join("\t", $type, $start_tm, $end_tm, $sz, $status, $info);
      $upd = 1;
      last;
    }
  }
  unless($upd) {
    push @all_stats, join("\t", $type, $start_tm, $end_tm, $sz, $status, $info);
  }
}

sub doMonitor {
  my ($type, $cnt) = split(/\s+/, $params);
  my (@all_stats, $i) = ((), 0);
  tie @all_stats, 'Tie::File', $stats_db or &printAndDie("ERROR", "unable to open the stats database $stats_db");
  foreach my $stat (@all_stats) {
    my ($stype, $sstart, $send, $ssize, $sstatus, $info) = split(/\t/, $stat);
    if($stype eq $type) {
      print STDOUT $stat, "\n";
      $i++;
    }
    if($i == $cnt) {
      last;
    }
  }
}

&printLog( "Client started\n" );
STDIN->autoflush(1);
STDOUT->autoflush(1);
&getInputs();

# xtrabackup  Ver 0.9 Rev 83 for 5.0.84 unknown-linux-gnu (x86_64)
eval {
  unless(Which::which('xtrabackup') and Which::which('innobackupex-1.5.1')) {
    &printToServer("ERROR", "xtrabackup is not properly installed, or not in \$PATH.");
  }
  $_ = qx/xtrabackup --version 2>&1/;
  if(/^xtrabackup\s+Ver\s+(\d+\.\d+)/) {
    if($MIN_XTRA_VERSION > $1) {
      &printAndDie("ERROR", "xtrabackup is not of the minimum required version: $MIN_XTRA_VERSION > $1.");
    }
  }
  else {
    &printAndDie("ERROR", "xtrabackup did not return a valid version string");
  }
};
if($@) {
  chomp($@);
  &printAndDie("ERROR", "xtrabackup not present or otherwise not executable. $@");
}


if( $action eq "copy from" ){
  if(-f "/tmp/zrm-innosnap/running" ) {
    &printLog(" Redirecting to innobackupex. \n");
    open FAKESNAPCONF, "</tmp/zrm-innosnap/running";
    $_ = <FAKESNAPCONF>; # timestamp
    chomp($_);
    if((time - int($_)) >= 300) {
      &printLog("  Caught stale inno-snapshot - deleting.\n");
      unlink("/tmp/zrm-innosnap/running");
    }
    else {
      $_ = <FAKESNAPCONF>; # user
      chomp($_);
      $params .= " --user=$_ ";
      $mysql_user=$_;
      $_ = <FAKESNAPCONF>; # password
      chomp($_);
      $params .= " --password=$_ ";
      $mysql_pass=$_;

      $tmp_directory=&getTmpName();
      my $r = mkdir( $tmp_directory );
      if( $r == 0 ){
        &printAndDie( "Unable to create tmp directory $tmp_directory.\n$!\n" );
      }
      &doRealHotCopy( $tmp_directory );
    }
  }
  else {
    my @suf;
    my $file = basename( $params, @suf );
    my $dir = dirname( $params );
    &writeTarStream( $dir, $file );
  }
}elsif( $action eq "copy between" ){
  $tmp_directory=&getTmpName();
  my $r = mkdir( $tmp_directory );
  if( $r == 0 ){
    &printAndDie( "Unable to create tmp directory $tmp_directory.\n$!\n" );
  }
  &doCopyBetween();
}elsif( $action eq "copy to" ){
  if( ! -d $params ){
    &printAndDie( "$params not found\n" );
  }
  &readTarStream( $params );
}elsif( $action eq "snapshot" ){
  $tmp_directory=&getTmpName();
  my $r = mkdir( $tmp_directory, 0700 );
  &doSnapshotCommand( $tmp_directory );
}elsif( $action eq "monitor" ) {
  doMonitor();
}else{
  $PL->i("Unknown action: $action, ignoring.");
}
&printLog( "Client clean exit\n" );
&my_exit( 0 );

1;
