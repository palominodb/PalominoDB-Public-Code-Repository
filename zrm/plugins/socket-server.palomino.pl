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

use File::Path;
use File::Basename;
use File::Temp qw/ :POSIX /;
use IO::Select;
use IO::Handle;
use Sys::Hostname;
use POSIX;

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
my $VERSION="1.8b6_palomino";
my $logDir = "/var/log/mysql-zrm";
my $logFile = "$logDir/socket-server.log";
my $snapshotInstallPath = "/usr/share/mysql-zrm/plugins";


my $nagios_service = "MySQL Backups";
my $nagios_host = "nagios.example.com";
my $nsca_client = "/usr/sbin/send_nsca";
my $nsca_cfg = "/usr/share/mysql-zrm/plugins/zrm_nsca.cfg";

if( -f "/usr/share/mysql-zrm/plugins/socket-server.conf" ) {
  open CFG, "< /usr/share/mysql-zrm/plugins/socket-server.conf";
  while(<CFG>) {
    my ($var, $val) = split /\s+/, $_, 1;
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
  }
}

open LOG, ">>$logFile" or die "Unable to create log file";
LOG->autoflush(1);
#$SIG{'PIPE'} = sub { &printAndDie( "pipe broke\n" ); };

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

	my ($fhs, $buf);
	POSIX::mkfifo("/tmp/innobackupex-log", 0700);
	&printLog("Created FIFOS..\n");

	open(INNO_TAR, "$MYSQL_BINPATH/$INNOBACKUPEX $new_params --stream=tar $tmp_directory 2>/tmp/innobackupex-log|");
	&printLog("Opened InnoBackupEX.\n");
	open(INNO_LOG, "</tmp/innobackupex-log");
	&printLog("Opened Inno-Log.\n");
	$fhs = IO::Select->new();
	$fhs->add(\*INNO_TAR);
	$fhs->add(\*INNO_LOG);
	while( $fhs->count() > 0 ) {
		my @r = $fhs->can_read(5);
		foreach my $fh (@r) {
			if($fh == \*INNO_LOG) {
				if( sysread( INNO_LOG, $buf, 10240 ) ) {
					&printLog($buf);
					if($buf =~ /innobackupex: Error:(.*)/ || $buf =~ /Pipe to mysql child process broken:(.*)/) {
						sendNagiosAlert("CRITICAL: $1", 2);
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
	my $fileList = $_[1];
	my $lsCmd = "";

	my $tmpFile = getTmpName();

	if( $_[1] =~ /\*/){
		$lsCmd = "cd $_[0]; $LS -1 $_[1] > $tmpFile 2>/dev/null;";
		my $r = system( $lsCmd );
		$fileList = " -T $tmpFile";
	}

	&printLog("writeTarStream: $TAR $TAR_WRITE_OPTIONS $_[0] $fileList");
	unless(open( TAR_H, "$TAR $TAR_WRITE_OPTIONS $_[0] $fileList 2>/dev/null|" ) ){
		&printAndDie( "tar failed $!\n" );
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
	&printLog("readTarStream: $TAR $TAR_READ_OPTIONS $_[0]");
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
		&printLog("createLink: $p\n");
		&printLog("createLink: $link\n");
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
	my $cmd = "/usr/share/mysql-zrm/plugins/socket-copy.palomino.pl $params > $tmp_directory/$ofile 2>$tmp_directory/$efile";
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
	if(-f "/tmp/zrm-innosnap/running" ) {
		&printLog(" Redirecting to innobackupex. \n");
		open FAKESNAPCONF, "</tmp/zrm-innosnap/running";
		$_ = <FAKESNAPCONF>; # Throw away the timestamp for now
		$_ = <FAKESNAPCONF>;
		chomp($_);
		$params .= " --user=$_ ";
		$_ = <FAKESNAPCONF>;
		chomp($_);
		$params .= " --password=$_ ";

		$tmp_directory=&getTmpName();
		my $r = mkdir( $tmp_directory );
		if( $r == 0 ){
			&printAndDie( "Unable to create tmp directory $tmp_directory.\n$!\n" );
		}
		&doRealHotCopy( $tmp_directory );
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
}elsif( $action eq "mysqlhotcopy" ){
	$tmp_directory=&getTmpName();
	my $r = mkdir( $tmp_directory );
	if( $r == 0 ){
		&printAndDie( "Unable to create tmp directory $tmp_directory.\n$!\n" );
	}
	&doRealHotCopy( $tmp_directory );
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

