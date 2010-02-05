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

# ###########################################################################
# ProcessLog package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessLog package
# ###########################################################################

package main;

use strict;
use Socket;
use File::Temp qw/ :POSIX /;
use Getopt::Long;
use File::Basename;
use File::Spec::Functions;
use Data::Dumper;


$ENV{PATH}="/usr/local/bin:/opt/csw/bin:/usr/bin:/usr/sbin:/bin:/sbin";
# xinetd port on remote host 
# Set socket-remote-port in mysql-zrm.conf if this needs to be changed.
my $REMOTE_PORT = 25300;

# Set remote-mysql-binpath if mysql client binaries on the remote machine 
# are in a different location.
my $REMOTE_MYSQL_BINPATH = "/usr/bin";

# This is the temporary directory on the remote host
# that is used to keep data before transferring to the backup host.
# This gets inherited from mysql-zrm. If that should not be used then
# Uncomment and modify this if some other directory is to be used
#$ENV{'TMPDIR'}="/tmp";

my $TAR = "tar";
my $TAR_WRITE_OPTIONS = "";
my $TAR_READ_OPTIONS = "";
my $CP="cp -pr";

my $MYSQL_BINPATH="/usr/bin";
my $MYSQLHOTCOPY="mysqlhotcopy";

my $VERSION = "1.8b7_palomino";
my $srcHost = "localhost";
my $destHost = "localhost";
my $destDir;
my $srcFile;
my $action;
my $params;
my $host;
my @snapshotParamList;
my $snapshotConfString;
my %config;
my $pl; # ProcessLog

$SIG{'PIPE'} = sub { $pl->end; die "Pipe broke"; };
$SIG{'TERM'} = sub { close SOCK; $pl->end; die "TERM broke\n"; };

if($^O eq "linux") {
	$TAR_WRITE_OPTIONS = "--same-owner -cphsC";
	$TAR_READ_OPTIONS = "--same-owner -xphsC";
}
elsif($^O eq "freebsd") {
	$TAR_WRITE_OPTIONS = " -cph -f - -C";
	$TAR_READ_OPTIONS = " -xp -f - -C";
}
else {
  &printAndDie("Unable to determine which tar options to use!");
}

sub printAndDie {
  $pl->e(@_);
  $pl->end;
  die("ERROR: @_");
}

sub my_exit {
  $pl->end;
  exit($_[0]);
}

# Parses the command line for all of the copy parameters
sub getCopyParameters()
{
	my %opt;
	my $ret = GetOptions( \%opt,
		"source-host=s",
		"source-file=s",
		"create-link",
		"destination-host=s",
		"destination-directory=s" );

	unless( $ret ){
		die( "Invalid parameters" );
	}

	if( !$opt{"source-file"} ){
		die( "No source file defined" );
	}else{
		$srcFile=$opt{"source-file"};
	}

	if( !$opt{"destination-directory"} ){
		die( "No destination file defined" );
	}else{
		$destDir=$opt{"destination-directory"};
	}	

	if( $opt{"source-host"} ){
		$srcHost = $opt{"source-host"};	
	}

	if( $opt{"destination-host"} && 
		$opt{"destination-host"} ne "localhost" ){
		$destHost = $opt{"destination-host"};
	}

	if( $srcHost eq "localhost" && $destHost eq "localhost" ){
		&doLocalTar();
		my_exit(0);
	}

	if( defined $opt{"create-link"} ){
		$action = "create-link";
		$params = $srcFile;
		$host = $destHost;
	}else{

		if( $srcHost ne "localhost" && $destHost ne "localhost" ){
			$action = "copy between";
			$host = $srcHost;
			my $d = $destHost;
			if( $destHost eq $srcHost ){
				$d = "localhost";	
			}
			$params = "--source-file '$srcFile' --destination-dir $destDir --source-host localhost --destination-host $d"; 
		}else{
			if( $srcHost ne "localhost" ){
				$action = "copy from";
				$params = $srcFile;
				$host = $srcHost;
			}else{
				$action = "copy to";
				$params = $destDir;
				$host = $destHost;
			}
		}
	}
	$pl->m("socket-copy:\taction:$action\n\tsrcHost:$srcHost\n\tparams:$params\n\tdestHost:$destHost\n\tdestDir:$destDir");
}

sub doLocalTar()
{
	my $cmd;
	my $tarCmd = $^O eq "linux" ? "$TAR --same-owner -psC " : "$TAR -pC";

  if( $config{'tar-force-ownership'} == 0 ) {
    if($^O eq 'linux') {
      $tarCmd = "$TAR --no-same-owner --no-same-permissions -sC";
    }
    elsif($^O eq 'freebsd') {
      $tarCmd = "$TAR -C";
    }
  }

	my $srcDir = dirname( $srcFile );
	my $srcFile = basename( $srcFile );	

	my $d = tmpnam();

	my $fileList = $srcFile;
	my $lsCmd = "";
	if( $srcFile =~ /\*/){
		$lsCmd = "cd $srcDir; ls -1 $srcFile > $d 2>/dev/null;";
		$fileList = " -T $d";
	}

	my $srcCmd = "$lsCmd $tarCmd $srcDir -h -c $fileList";
	my $destCmd = "$tarCmd $destDir -x";
	$cmd = "$srcCmd|$destCmd";

  $pl->m("local-tar:\n\t$cmd");

	my $r = system( $cmd );
	if( $lsCmd ne "" ){
		unlink $d;
	}
	if( $r > 0 ){
    &printAndDie("Could not copy data $!");
	}
}

# Parses the command line ofr the mysqlhotcopy parameters
sub getMySQLHotCopyParameters()
{
	my $y = shift @ARGV;
	my @x = split( /=/, $y );
	my $l = @x;
	if( $l > 1 ){
		$MYSQL_BINPATH = $x[1];
	}
	$destDir= pop @ARGV;
	my %opt;
	GetOptions( \%opt,
		"host=s",
		"user=s",
		"password=s",
		"socket=s",
		"port=s",
		"quiet" );
	$params = "";
	for( keys%opt ){
		if( $_ ne "quiet" ){
			$params .= " --$_=\"$opt{$_}\"";
		}else{
			$params .= " --$_";
		}
	}
	foreach( @ARGV ){
		$params .= " $_";
	}
	if( $opt{"host"} ){
		$host = $opt{"host"};
	}

	if( ! $host || $host eq "localhost" ){
		my_exit( system( "$MYSQL_BINPATH/mysqlhotcopy $params $destDir" ) );
	}
	$action = "mysqlhotcopy";
}

#gets parameters for remove backup data
sub getRemoveBackupParams()
{
	my $y = shift @ARGV;
	my %opt;
	GetOptions( \%opt,
		"host=s",
		"backup-dir=s",
		"type-of-dir=s",
		"backup-id=i" );
	$host = $opt{"host"};
	my $dir = $opt{"backup-dir"};
	my $id;
	if( defined $opt{"backup-id"} ){
		$id = $opt{"backup-id"};
	}else{
		$id = $opt{"type-of-dir"};
	}
	$params = "$id $dir";

	$action = "remove-backup-data";
}

sub getSnapshotParams()
{
	my $y = shift @ARGV;
	my %opt;
	GetOptions( \%opt,
		"host=s",
		"snapshot-parameters=s" );
	$host = $opt{"host"};
	$params = $opt{"snapshot-parameters"};
	$action = "snapshot";
}

# This will parse the command line arguments
sub getInputs()
{
	my $len = @ARGV;
	if( $len == 0 ){
		die "This plugin is meant to be invoked from mysql-zrm only\n";
	}

	if( $ARGV[0]=~/^--mysqlhotcopy/ ){
		getMySQLHotCopyParameters();
	}elsif( $ARGV[0]=~/^remove-backup-data/ ){
		getRemoveBackupParams();
	}elsif( $ARGV[0]=~/^--snapshot-command/ ){
		getSnapshotParams();
	}else{
		getCopyParameters();
	}
}	


#This will opne the connection to the remote host
sub connectToHost()
{
  $pl->m('connect-to-host:\thost:', $host, '\tport:', $REMOTE_PORT);
	my $iaddr = inet_aton($host) or die "no host: $host";
	my $paddr = sockaddr_in($REMOTE_PORT, $iaddr);
	my $proto = getprotobyname('tcp');
	socket(SOCK, PF_INET, SOCK_STREAM, $proto) or die "socket: $!";
	connect(SOCK, $paddr) or die "connect: $!";
	select( SOCK );
	$| = 1;
	select( STDOUT );
  $pl->m('connected to host.');
}

# This will send the required arguments to the remote host
sub sendArgsToRemoteHost()
{
	my $tmp=File::Spec->tmpdir();
  $pl->m('send-args-to-host:\n', join("\n  ", ($VERSION, $action, $params, $tmp, $REMOTE_MYSQL_BINPATH)));
	print SOCK "$VERSION\n";
	print SOCK "$action\n";
	print SOCK "$params\n";
	print SOCK "$tmp\n";
	print SOCK "$REMOTE_MYSQL_BINPATH\n";
}

# This will read the data from the socket and pipe the output to tar
sub readTarStream()
{
  my $tmpfile = tmpnam();
  my $tar_cmd = "|$TAR $TAR_READ_OPTIONS $destDir 2>$tmpfile";
  $pl-m("read-tar-stream:\n\t$tar_cmd\n");
	unless( open( TAR_H, "$tar_cmd" ) ){
		&printAndDie("tar failed $!");
	}
	binmode( TAR_H );

	my $buf;

	# Initially read the length of data to read
	# This will be packed in network order
	# Then read that much data which is uuencoded
	# Then write the unpacked data to tar
	while( read( SOCK, $buf, 4 ) ){
		$buf = unpack( "N", $buf );
		read SOCK, $buf, $buf;
		print TAR_H unpack( "u", $buf );
	}
  {
    local $/;
    open my $fh, "<$tmpfile";
    my $errs = <$fh>;
    chomp($errs);
    $pl->e("tar-errors:", $errs) if($errs !~ /\s*/);
    close $fh;
    unlink $tmpfile;
  }
	unless( close(TAR_H) ){
    &printAndDie('tar pipe failed');
	}
}

# This will read the data from the socket and pipe the output to tar
sub readInnoBackupStream()
{
  my $tar_cmd = "|$TAR ";
  my $tmpfile = tmpnam();
  if( $config{'tar-force-ownership'} == 0 ) {
    $tar_cmd .= "--no-same-owner --no-same-permissions -xiC ";
  }
  else {
    $tar_cmd .= "--same-owner -xipC ";
  }
  $tar_cmd .= "$destDir 2>$tmpfile";
  $pl->m("read-inno-tar-stream:", $tar_cmd);
  #print "read-inno-tar-stream:\n\t$tar_cmd\n";

	unless( open( TAR_H, "$tar_cmd" ) ){
    &printAndDie("tar failed $!");
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
    open my $fh, '<', $tmpfile;
    my $errs = <$fh>;
    chomp($errs);
    $pl->e("tar-errors:", $errs);# if($errs !~ /\s*/);
    close $fh;
    unlink $tmpfile;
  }
	unless( close(TAR_H) ){
    &printAndDie("tar pipe failed");
	}

  if( $config{'apply-xtrabackup-logs'} == 1 ) {
    $pl->m("Applying logs..");
    $pl->x(\&system, "innobackup-1.5.1 --apply-log $destDir");
  }
}

#This will tar the directory and write output to the socket
#$_[0] dirname
#$_[1] filename
sub writeTarStream()
{
	unless(open( TAR_H, "$TAR $TAR_WRITE_OPTIONS $_[0] $_[1] 2>/dev/null|" ) ){
		&printAndDie( "tar failed $!\n" );
	}
	binmode( TAR_H );
	my $buf;
	while( read( TAR_H, $buf, 10240 ) ){
		my $x = pack( "u*", $buf );
		print SOCK pack( "N", length( $x ) );
		print SOCK $x;
	}
	close( TAR_H );
}

#Read the config file
# This reads the conf file that is prepared by mysql-zrm.
# Please note this does not do any validation of the config file
# pointed to by $ZRM_CONF in the enviornment
sub parseConfFile()
{
	my $fileName = $ENV{'ZRM_CONF'};
	unless( open( FH, "$fileName" ) ){
		die "Unable to open config file. This should only meant to be invoked from mysql-zrm\n";
	}
	my @tmparr = <FH>;
	close( FH );
	chomp( @tmparr );
	foreach( @tmparr ){
		my @v = split( /=/, $_ );
		my $v1 = shift @v;
		my $v2 = join( "=", @v );
		$config{$v1} = $v2;
	}
}

# Setup the parameters that are relevant from the conf 
sub setUpConfParams()
{
  $pl->m('setup-config-parameters');
	if( $config{"socket-remote-port"} ){
		$REMOTE_PORT = $config{"socket-remote-port"};
	}
	if( $config{"remote-mysql-binpath"} ){
		$REMOTE_MYSQL_BINPATH = $config{"remote-mysql-binpath"};
	}
	if( defined $ENV{'SNAPSHOT_CONF'} ){
    $pl->m('read-snapshot-config');
		my $fName = $ENV{'SNAPSHOT_CONF'};
		unless( open( TMP, $fName ) ){
			return;
		}
		@snapshotParamList = <TMP>;
		chomp( @snapshotParamList );
		close TMP;
		unlink( $fName );
		$snapshotConfString = "";
		foreach(@snapshotParamList){
			$snapshotConfString .= "$_=$config{$_}\n";
		}
    $pl->m('snapshot-param-list:', Dumper(\@snapshotParamList));
    $pl->m('snapshot-param-string:', $snapshotConfString);
	}
}

sub doCreateLinks()
{
	unless( open( TP, $params ) ){
		die "unable to open input file\n";
	}
	my @l = <TP>;
	close TP;
	chomp( @l );
	unlink $params;
	my $n = @l;
	print SOCK "$n\n";
	foreach( @l ){
		print SOCK "$_\n";
	}

	my $status = <SOCK>;
	my $r = <SOCK>;
	if( $r eq "SUCCESS" ){
		print STDOUT "$r\n";
	}else{
		print STDERR "$r\n";
	}
}

sub doSnapshotCommand()
{
  $pl->m("do-snapshot:\tplugin:",$config{'snapshot-plugin'});
	print SOCK $config{"snapshot-plugin"}."\n";
	my $num = @snapshotParamList;
	$num += 2; # For user/pass
	print SOCK "$num\n";
	if( $num > 0 ){
		print SOCK "$snapshotConfString";
		print SOCK "user=$config{'user'}\n";
		print SOCK "password=$config{'password'}\n";
	}
  $pl->m('  sent config data.');
	my $status = <SOCK>;
	chomp( $status );
  $pl->m('  result:', $status);
	my $num = <SOCK>;
	chomp($num);
	my $i;
	for( $i = 0 ; $i < $num; $i++ ){
		my $r = <SOCK>;	
		if( $status eq "SUCCESS" ){
      $pl->m(' ', $r);
			print STDOUT $r;
		}else{
			print STDERR $r;
      $pl->e(' ', $r);
		}
	}
	if( $status ne "SUCCESS" ){
		my_exit(1);
	}
}

sub doCopyBetween()
{
	print SOCK "$REMOTE_PORT\n"; 
	my $status = <SOCK>;
	chomp( $status );
	my $num = <SOCK>;
	chomp($num);
	my $i;
	for( $i = 0 ; $i < $num; $i++ ){
		my $r = <SOCK>;	
		if( $status eq "SUCCESS" ){
			print STDOUT $r;
		}else{
			print STDERR $r;
		}
	}
	if( $status ne "SUCCESS" ){
		my_exit(1);
	}

}

&parseConfFile();
&setUpConfParams();

unless( exists $config{'socket-copy-logfile'} ) {
  $config{'socket-copy-logfile'} = '/var/log/mysql-zrm/socket-copy.log';
}
unless( exists $config{'socket-copy-email'} ) {
  $config{'socket-copy-email'} = undef;
}

$pl = ProcessLog->new('socket-copy', $config{'socket-copy-logfile'}, $config{'socket-copy-email'});
$pl->quiet(1); # Hide messages from the console.
$pl->start;

if( $config{"tar-force-ownership"} == 0 or $config{"tar-force-ownership"} =~ /[Nn][oO]?/ ) {
  $config{"tar-force-ownership"} = 0;
  if( $^O eq "linux" ) {
    $TAR_WRITE_OPTIONS = "--no-same-owner --no-same-permissions -chsC";
    $TAR_READ_OPTIONS = "---no-same-owner --no-same-permissions -xhsC";
  }
  elsif( $^O eq "freebsd" ) {
    $TAR_WRITE_OPTIONS = " -ch -f - -C";
    $TAR_READ_OPTIONS  = " -x -f - -C";
  }
}

&getInputs();
&connectToHost();
&sendArgsToRemoteHost();
if( $action eq "copy from" ){
	&readInnoBackupStream();
}elsif( $action eq "mysqlhotcopy" ){
  &printAndDie("InnobackupEX is hotcopy. No need for mysqlhotcopy.");
}elsif( $action eq "copy between" ){
	&doCopyBetween();
}elsif( $action eq "copy to" ){
	my @suf;
	my $file = basename( $srcFile, @suf );
	my $dir = dirname( $srcFile );
	&writeTarStream( $dir, $file );
}elsif( $action eq "create-link" ){
	&doCreateLinks( );
}elsif( $action eq "snapshot" ){
	&doSnapshotCommand( $params );
}elsif( $action ne "remove-backup-data" ){
	die "Unknown action";
}
close( SOCK );
select( undef, undef, undef, 0.250 );
my_exit(0);
1;
