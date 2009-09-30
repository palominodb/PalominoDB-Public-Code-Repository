#!/usr/bin/perl
use strict;
use warnings;
use IO::Select;
use IO::Handle;
use POSIX;
use Data::Dumper;
use Sys::Hostname;

my $MYSQL_BINPATH = $^O eq "freebsd" ? "/usr/local/bin" : "/usr/bin";
my $INNOBACKUPEX = "innobackupex-1.5.1";
my $tmp_directory = "/tmp";
my $new_params = "--user root --password pass";

my $nagios_service = "";
my $nsca_client = "";
my $nsca_cfg = "";
my $nagios_host = "";

sub printLog {
  print(scalar(localtime()) . ' [DEBUG]: '. $_[0]);
}

sub printAndDie {
  print(scalar(localtime()) . ' [DIE]: '. $_[0]);
}

sub doRealHotCopy()
{
	# massage params for innobackup
  #$params =~ s/--quiet//;
	#my $new_params = "";
	#foreach my $i (split(/\s+/, $params)) {
	#	&printLog("param: $i\n");
	#	next if($i !~ /^--/);
	#	next if($i =~ /^--host/);
	#	$new_params .= "$i ";
	#}

	my ($fhs, $buf);
	POSIX::mkfifo("/tmp/innobackupex-log", 0700);
	&printLog("Created FIFO..\n");

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
					&printLog("closed log handle normally\n");
					$fhs->remove($fh);
					close(INNO_LOG);
				}
			}
			if($fh == \*INNO_TAR) {
				if( sysread( INNO_TAR, $buf, 10240 ) ) {
          #my $x = pack( "u*", $buf );
					#print STDOUT pack( "N", length( $x ) );
					#print STDOUT $x;
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
	&printLog("Pinging nagios with: echo -e '$host\t$nagios_service\\t$status\\t$alert' | $nsca_client -c $nsca_cfg -H $nagios_host\n");
  #$_ = qx/echo -e '$host\\t$nagios_service\\t$status\\t$alert' | $nsca_client -c $nsca_cfg -H $nagios_host/;
}

&doRealHotCopy();
