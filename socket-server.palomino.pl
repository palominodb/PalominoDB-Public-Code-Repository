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

use File::Path;
use File::Basename;
use File::Temp qw/ :POSIX /;

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
my $LS = "ls";

my $TMPDIR;
my $tmp_directory;
my $action;
my $params;
my $INNOBACKUPEX="innobackupex-1.5.1";
my $VERSION="1.8";
my $logDir = "/var/log/mysql-zrm";
my $logFile = "$logDir/socket-server.log";
my $snapshotInstallPath = "/usr/share/mysql-zrm/plugins";

open LOG, ">>$logFile" or die "Unable to create log file";
$SIG{'PIPE'} = sub { &printAndDie( "pipe broke\n" ); };

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

sub printLog()
{
	print LOG $_[0];
}

sub printAndDie()
{
	&printLog( "ERROR: $_[0]" );
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

sub doHotCopy()
{
#	my $r = system( "$MYSQL_BINPATH/$MYSQLHOTCOPY $params $tmp_directory 2>/dev/null" );
#	if( $r > 0 ){
#		&printAndDie( "mysqlhotcopy failed $!\n" );
#	}
	$params =~ s/host/remote-host/;
	$params =~ s/--quiet//;
	my $new_params = "";
	foreach my $i (split(/\s+/, $params)) {
		&printLog("param: $i\n");
		next if($i !~ /^--/);
		$new_params .= "$i ";
	}
	&printLog("HOT COPY COMMAND:$MYSQL_BINPATH/$INNOBACKUPEX $new_params $tmp_directory &>/tmp/innobackup.log\n");
	my $r = system("$MYSQL_BINPATH/$INNOBACKUPEX $new_params $tmp_directory &>/tmp/innobackup.log");
	if( $r > 0 ){
		&printAndDie("innobackupex failed $1\n");
	}
}

sub doRealHotCopy()
{
	# massage params for innobackup
	$params =~ s/--quiet//;
	my $new_params = "";
	foreach my $i (split(/\s+/, $params)) {
		&printLog("param: $i\n");
		next if($i !~ /^--/);
		next if($i =~ /^--host/);
		$new_params .= "$i ";
	}
	&printLog("HOT COPY COMMAND:$MYSQL_BINPATH/$INNOBACKUPEX $new_params --stream=tar $tmp_directory\n");
	unless(open( TAR_H, "$MYSQL_BINPATH/$INNOBACKUPEX $new_params --stream=tar $tmp_directory 2>/tmp/innobackup.log|" ) ) {
		&printandDie( "tar failed $!\n" );
	}
	binmode( TAR_H );
	my $buf;
	while( read( TAR_H, $buf, 10240 ) ){
		my $x = pack( "u*", $buf );
		print pack( "N", length( $x ) );
		print $x;
	}
	close( TAR_H );
}

#$_[0] dirname
#$_[1] filename
sub writeTarStream()
{
	my $fileList = $_[1];
	my $lsCmd = "";

	my $tmpFile = getTmpName();

	if( $_[1] =~ /\*/){
		$lsCmd = "cd $_[0]; $LS -1 $_[1] > $tmpFile 2>/dev/null;";
		my $r = system( $lsCmd );
		$fileList = " -T $tmpFile";
	}

	unless(open( TAR_H, "$TAR --same-owner -cphszC $_[0] $fileList 2>/dev/null|" ) ){
		&printandDie( "tar failed $!\n" );
	}
	binmode( TAR_H );
	my $buf;
	while( read( TAR_H, $buf, 10240 ) ){
		my $x = pack( "u*", $buf );
		print pack( "N", length( $x ) );
		print $x;
	}
	close( TAR_H );

	if( $lsCmd ){
		unlink( $tmpFile );
	}
}

#$_[0] dirname to strea the data to
sub readTarStream()
{
	unless(open( TAR_H, "|$TAR --same-owner -xphszC $_[0] 2>/dev/null" ) ){
		&printandDie( "tar failed $!\n" );
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


sub removeBackupData()
{
	my @sp = split /\s/, $params;
	my $id = $sp[0];
	shift @sp;
	my $dir = join( /\s/, @sp );
	my $orig = $dir;
	if( $id eq "LINKS" ){
		$dir .= "/ZRM_LINKS";
	}elsif( $id eq "MOUNTS" ){
		$dir .= "/ZRM_MOUNTS";
		if( ! -d $dir ){
			return;
		}
	}else{
		$dir .= "/BACKUP/BACKUP-$id";
	}
	rmtree $dir, 0, 0;
	if( $id eq "MOUNTS" ){
		rmdir $orig;
	}
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
	&printLog( "status=$status\n" );
	print "$status\n";
	print "$cnt\n";
	my $i;
	for( $i = 0; $i < $cnt; $i++ ){
		&printLog( "$data[$i]\n" );
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

sub doCreateLink()
{
	my $num = &readOneLine();
	my $i;
	my $er = 0;
	for( $i = 0; $i < $num; $i += 2 ){
		my $p = &readOneLine();
		my $link = &readOneLine();
		mkpath( $p, 0, 0700 );
		my $cmd = "ln -s $link";
		my $r = system( $cmd );
		if( $r != 0 ){
			$er = 1;
			&printFileToServer( "ERROR", "Could not create link." );
		}
	}
	if( $er == 0 ){
		&printFileToServer( "SUCCESS", "Link created." );	
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
	my $cmd = "/usr/share/mysql-zrm/plugins/socket-copy.pl $params > $tmp_directory/$ofile 2>$tmp_directory/$efile";
	&printLog( "$cmd\n" );
	my $r = system( $cmd );
	if( $r == 0 ){
		&printFileToServer( "SUCCESS", "$tmp_directory/$ofile" );	
	}else{
		&printFileToServer( "ERROR", "$tmp_directory/$efile" );
	}
}

&printLog( "Client started\n" );
select( STDIN );
$| = 1;
select( STDOUT );
$| = 1;
&getInputs();

if( $action eq "copy from" ){
	my @suf;
	my $file = basename( $params, @suf );
	my $dir = dirname( $params );
	&writeTarStream( $dir, $file );
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
}elsif( $action eq "mysqlhotcopy" ){
	$tmp_directory=&getTmpName();
	my $r = mkdir( $tmp_directory );
	if( $r == 0 ){
		&printAndDie( "Unable to create tmp directory $tmp_directory.\n$!\n" );
	}
	#&doHotCopy( $tmp_directory );
	&doRealHotCopy( $tmp_directory );
	#&writeTarStream( $tmp_directory, "." );
}elsif( $action eq "remove-backup-data" ){
	&removeBackupData();
}elsif( $action eq "snapshot" ){
	$tmp_directory=&getTmpName();
	my $r = mkdir( $tmp_directory, 0700 );
	&doSnapshotCommand( $tmp_directory );
}elsif( $action eq "create-link" ){
	&doCreateLink();
}else{
	&printAndDie( "Unknown action $action\n" );
}
&printLog( "Client clean exit\n" );
&my_exit( 0 );

