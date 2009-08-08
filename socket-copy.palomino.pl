#!/usr/bin/perl
#
# Copyright (c) 2006 Zmanda Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
#
# Contact information: Zmanda Inc, 505 N Mathlida Ave, Suite 120
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
#

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
use Socket;
use File::Temp qw/ :POSIX /;
use Getopt::Long;
use File::Basename;
use File::Spec::Functions;


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
my $CP="cp -pr";

my $MYSQL_BINPATH="/usr/bin";
my $MYSQLHOTCOPY="mysqlhotcopy";

my $VERSION = "1.8";
my $srcHost = "localhost";
my $destHost = "localhost";
my $destDir;
my $srcFile;
my $action;
my $params;
my $host;
my @snapshotParamList;
my $snapshotConfString;

$SIG{'PIPE'} = sub { die "Pipe broke"; };
$SIG{'TERM'} = sub { close SOCK; die "TERM broke\n"; };

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
		exit(0);
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
	print "socket-copy params: $params\n"
}

sub doLocalTar()
{
	my $cmd;
	my $tarCmd = "$TAR --same-owner -phszC ";

	my $srcDir = dirname( $srcFile );
	my $srcFile = basename( $srcFile );	

	my $d = tmpnam();

	my $fileList = $srcFile;
	my $lsCmd = "";
	if( $srcFile =~ /\*/){
		$lsCmd = "cd $srcDir; ls -1 $srcFile > $d 2>/dev/null;";
		$fileList = " -T $d";
	}

	my $srcCmd = "$lsCmd $tarCmd $srcDir -c $fileList";
	my $destCmd = "$tarCmd $destDir -x";
	$cmd = "$srcCmd|$destCmd";

	my $r = system( $cmd );
	if( $lsCmd ne "" ){
		unlink $d;
	}
	if( $r > 0 ){
		die "Could not copy data $!\n";
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
		exit( system( "$MYSQL_BINPATH/mysqlhotcopy $params $destDir" ) );
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
	my $iaddr = inet_aton($host) or die "no host: $host";
	my $paddr = sockaddr_in($REMOTE_PORT, $iaddr);
	my $proto = getprotobyname('tcp');
	socket(SOCK, PF_INET, SOCK_STREAM, $proto) or die "socket: $!";
	connect(SOCK, $paddr) or die "connect: $!";
	select( SOCK );
	$| = 1;
	select( STDOUT );
}

# This will send the required arguments to the remote host
sub sendArgsToRemoteHost()
{
	my $tmp=File::Spec->tmpdir();
	print SOCK "$VERSION\n";
	print SOCK "$action\n";
	print SOCK "$params\n";
	print SOCK "$tmp\n";
	print SOCK "$REMOTE_MYSQL_BINPATH\n";
}

# This will read the data from the socket and pipe the output to tar
sub readTarStream()
{
	unless( open( TAR_H, "|$TAR --same-owner -xphszC $destDir 2>/dev/null" ) ){
	#unless( open( TAR_H, "|$TAR --same-owner -xipC $destDir 2>/dev/null" ) ){
		die "tar failed $!";
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
	unless( close(TAR_H) ){
		die "close of pipe failed\n";
	}
}

# This will read the data from the socket and pipe the output to tar
sub readInnoBackupStream()
{
	unless( open( TAR_H, "|$TAR --same-owner -xipC $destDir 2>/dev/null" ) ){
		die "tar failed $!";
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
	unless( close(TAR_H) ){
		die "close of pipe failed\n";
	}
}

#This will tar the directory and write output to the socket
#$_[0] dirname
#$_[1] filename
sub writeTarStream()
{
	unless(open( TAR_H, "$TAR --same-owner -cphszC $_[0] $_[1] 2>/dev/null|" ) ){
		&printandDie( "tar failed $!\n" );
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

my %config;
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
	if( $config{"socket-remote-port"} ){
		$REMOTE_PORT = $config{"socket-remote-port"};
	}
	if( $config{"remote-mysql-binpath"} ){
		$REMOTE_MYSQL_BINPATH = $config{"remote-mysql-binpath"};
	}
	if( defined $ENV{'SNAPSHOT_CONF'} ){
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
	print SOCK $config{"snapshot-plugin"}."\n";
	my $num = @snapshotParamList;
	print SOCK "$num\n";
	if( $num > 0 ){
		print SOCK "$snapshotConfString";
	}
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
		exit(1);
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
		exit(1);
	}

}

&parseConfFile();
&setUpConfParams();
&getInputs();
&connectToHost();
&sendArgsToRemoteHost();
if( $action eq "copy from" ){
	&readTarStream();
}elsif( $action eq "mysqlhotcopy" ){
	&readInnoBackupStream();
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
exit(0);
