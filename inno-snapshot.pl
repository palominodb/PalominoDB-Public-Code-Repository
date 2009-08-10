#!/usr/bin/perl
#
# Makes a fake snapshot so that ZRM doesn't try to do it's stupid
# raw/logical bullshit.
# This is called inno-snapshot.pl because it's supposed to be used
# with innobackupex.
# Seriously though, don't look through this thing. All it does is
# make a directory '/tmp/zrm-innosnap' with a file 'running'
# that contains a timestamp and the user/password for innobackupex.
# Yes, you read that right.
# This is because ZRM believes that if you make a snapshot you don't need to
# do anything other than copy. This is, of course, patently false, but
# who am I to disagree with the almighty ZRM?
#

use strict;
use warnings;
use Getopt::Long;
use File::Path;
use lib '/usr/lib/mysql-zrm';
use ZRM::SnapshotCommon;
use Data::Dumper;


$SIG{'TERM'} = sub { &printAndDie("TERM signal"); };

# Uses df to get the device name and filesystem type
# Uses lvdisplay to see if this device is an lvm volume
# $_[0] directory name
# $_[1] snapshot name
sub getSnapshotDeviceDetails()
{

  print "device=/dev/null\n";
  print "snapshot-device=/dev/null\n";
  print "device-mount-point=null\n";
  print "filesystem-type=null\n";
  my $str = &getCommonDetails( $_[0], $_[1], $_[0] );
  my @ret = split /\n/, $str;
  print $ret[1], "\n";
  print "snapshot-mount-point=/tmp/zrm-innosnap\n";
}

sub doGetSnapshotdeviceDetails()
{
	if( !defined $opt{"directory"} ){
		&printAndDie( "Please supply --directory" );
	} 
	if( !defined  $opt{"sname"} ) {
		&printAndDie( "Please supply --sname" );
	}
	&getSnapshotDeviceDetails( $opt{"directory"}, $opt{"sname"} );
}

sub doCreateSnapshot()
{
	mkdir("/tmp/zrm-innosnap");
	sleep(5);
	return;
}

sub doMount()
{
	open RUNFIL, ">/tmp/zrm-innosnap/running";
	print RUNFIL time, "\n";
	print RUNFIL "$ZRM::SnapshotCommon::config{'user'}\n";
	print RUNFIL "$ZRM::SnapshotCommon::config{'password'}\n";
	close RUNFIL;
	sleep(5);
	return;
}

sub doUmount()
{
	unlink("/tmp/zrm-innosnap/running");
	sleep(5);
	return;
}

sub doRemoveSnapshot()
{
	sleep(5);
	rmdir("/tmp/zrm-innosnap");
	return;
}

&initSnapshotPlugin();
if( $action eq "get-vm-device-details" ){
	&doGetSnapshotdeviceDetails();
}elsif( $action eq "create-snapshot" ){
	&doCreateSnapshot();
}elsif( $action eq "mount" ){
	&doMount();
}elsif( $action eq "umount" ){
	&doUmount();
}elsif( $action eq "remove-snapshot" ){
	&doRemoveSnapshot();
}else{
	&printAndDie( "Unknown action" );
}
exit( 0 );


