#!/usr/bin/perl
use strict;
use warnings;
use IPC::Open3;
use IO::Select;
use IO::Handle;
use POSIX;
use Data::Dumper;

my $MYSQL_BINPATH = "/usr/bin";
my $INNOBACKUPEX = "innobackupex-1.5.1";
my $tmp_directory = "/tmp";
my $new_params = "--user root --password pass";

$SIG{'PIPE'} = sub { print "disconnected pipe.\n", Dumper(\@_) };

$| = 1;

open LOG, ">/tmp/TEH_LOG";
open TAR, ">/tmp/bk.raw";

LOG->autoflush(1);
TAR->autoflush(1);

  my ($tar, $inno_in, $inno_err, $fhs, $buf, $tar_done, $log_done);
  $tar_done = 0;
  $log_done = 0;
  POSIX::mkfifo("/tmp/innobackupex-tar", 0700);
  POSIX::mkfifo("/tmp/innobackupex-log", 0700);
  #print("Created FIFOS..\n");

  #my $pid = fork;
  #if(!$pid) {
  #  close(STDOUT);
  #  close(STDERR);
  #  close(STDIN);
  system("$MYSQL_BINPATH/$INNOBACKUPEX $new_params --stream=tar $tmp_directory 2>/tmp/innobackupex-log 1>/tmp/innobackupex-tar &");
  print(LOG "Started InnobackupEX..\n");
  #exit(0);
  #  waitpid($pid, 0);
  #}
  #print "In parent, going to read from fifos $$\n";
  #my $pid = open3(\*NULL, $tar, \*LOG, "$MYSQL_BINPATH/$INNOBACKUPEX $new_params --stream=tar $tmp_directory");
  open INNO_TAR, "+</tmp/innobackupex-tar";
  #print "Opened TAR 1..\n";
  open INNO_LOG, "</tmp/innobackupex-log";
  #print "Opened LOG..\n";
  open INNO_TAR, "</tmp/innobackupex-tar";
  #print "Opened TAR 2..\n";
  binmode(INNO_TAR);
  #}
  $fhs = IO::Select->new();
  $fhs->add(\*INNO_TAR);
  $fhs->add(\*INNO_LOG);
  #$fhs->add($inno_err);
  #print("Got file handles: tar $tar, err $inno_err\n");
  #$fhs->add($inno_in);
  while($log_done == 0 || $tar_done == 0) {
    my @r_ready = $fhs->can_read();
    #print "fhs: ", scalar @r_ready, "\n";
    #if(scalar @r_ready == 0) {
    #  print "Testing FIFOS..\n";
    #  $tar_done = 1 if(sysread(INNO_TAR, $buf, 10240) == 0);
    #  $log_done = 1 if(sysread(INNO_LOG, $buf, 10240) == 0);
    #}
    foreach my $fh (@r_ready) {
      if($fh == \*INNO_TAR and $tar_done == 0) {
        print "Reading from TAR handle..\n";
        my $b = sysread($fh, $buf, 10240);
        if( $b == 0 ) {
          print "TAR DONE\n";
          $tar_done = 1;
          next;
        }
        #my $x = pack("u*", $buf);
        #print TAR pack("N", length($x));
        print TAR $buf;
      }
      if($fh == \*INNO_LOG and $log_done == 0) {
        print "Reading from LOG handle..\n";
        my $b = sysread($fh, $buf, 10240);
        if( $b == 0 ) {
          print "ALL DONE WITH TEH LOGZ.\n";
          $log_done = 1;
          next;
        }
        if($buf =~ /innobackupex: Error:(.*)/) {
          print "Big Error: $1\n";
          exit(1);
        }
        print LOG "$buf";
      }
    }
    #last if($tar_done and $log_done);
  }
  #waitpid($pid, 0);
  print "log_done: $log_done; tar_done: $tar_done\n";

close INNO_TAR;
close INNO_LOG;
close LOG;
unlink("/tmp/innobackupex-tar");
unlink("/tmp/innobackupex-log");
