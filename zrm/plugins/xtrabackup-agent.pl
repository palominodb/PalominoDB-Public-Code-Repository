#!/usr/bin/perl
# Copyright (c) 2010-2011 PalominoDB, Inc.  All Rights Reserved.
#
# Based on socket-copy.pl and socket-server.pl distributed in
# ZRM version 2.0 copyright Zmanda Inc.
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
# Contact information: PalominoDB Inc, 57 South Main St. #117
# Neptune, NJ 07753, United States, or: http://www.palominodb.com
#
# This is meant to be invoked by xinetd.
# See associated documentation for working details.
use strict;
use warnings FATAL => 'all';

# ###########################################################################
# ProcessLog package 5de7a9b2674ffd5aef20f2e8ff1cfca9c0308217
# ###########################################################################
package ProcessLog;
use strict;
use warnings FATAL => 'all';


my $mail_available = 1;
eval 'use Mail::Send';
if($@) {
  $mail_available = 0;
}
use Sys::Hostname;
use Sys::Syslog;
use Digest::MD5 qw(md5_hex);
use Time::HiRes qw(time);
use File::Spec;
use Fcntl qw(:seek);
use English qw(-no_match_vars);

use constant _PdbDEBUG => $ENV{Pdb_DEBUG} || 0;
use constant Level1 => 1;
use constant Level2 => 2;
use constant Level3 => 3;


sub new {
  my $class = shift;
  my ($script_name, $logpath, $email_to) = @_;
  my $self = {};

  $self->{run_id} = md5_hex(time . rand() . $script_name);

  $self->{script_name} = $script_name;
  $self->{log_path} = $logpath;
  $self->{email_to} = $email_to;
  $self->{stack_depth} = 10; # Show traces 10 levels deep.
  $self->{logsub} = 0;
  $self->{quiet} = 0;

  bless $self,$class;
  $self->logpath($logpath);
  return $self;
}


sub null {
  my $class = shift;
  $class->new('', '/dev/null', undef);
}


sub name {
  my $self = shift;
  $self->{script_name};
}


sub runid {
  my $self = shift;
  $self->{run_id};
}


sub start {
  my $self = shift;
  $self->m("BEGIN $self->{run_id}");
}


sub end {
  my $self = shift;
  $self->m("END $self->{run_id}");
}


sub stack_depth {
  my ($self, $opts) = @_;
  my $old = $self->{stack_depth};
  $self->{stack_depth} = $opts if( defined $opts );
  $old;
}


sub quiet {
  my ($self, $new) = @_;
  my $old = $self->{quiet};
  $self->{quiet} = $new if( defined $new );
  $old;
}


sub logpath {
  my ($self, $logpath) = @_;
  my $script_name = $$self{script_name};
  $self->{log_path} = $logpath;
  if($logpath =~ /^syslog:(\w+)/) {
    openlog($script_name, "", $1);
    $self->{logsub} = sub {
      my $self = shift;
      $_[3] = '';
      my $lvl = 'LOG_DEBUG';
      $lvl = 'LOG_INFO' if($_[0] eq "msg");
      $lvl = 'LOG_NOTICE' if($_[0] eq "ifo");
      $lvl = 'LOG_ERR'  if($_[0] eq "err");
      syslog($lvl, _p(@_));
      print _p(@_) unless $self->{quiet};
    };
  }
  elsif($logpath eq 'pdb-test-harness' or $logpath eq 'stderr') {
    $self->{logsub} = sub {
      my $self = shift;
      my @args = @_;
      $args[0] =~ s/^/# /;
      print STDERR _p(@args);
    }
  }
  else {
    open $self->{LOG}, ">>$self->{log_path}" or die("Unable to open logfile: '$self->{log_path}'.\n");
    $self->{logsub} = sub {
      my $self = shift;
      my $fh  = $self->{LOG};
      print $fh _p(@_);
      print _p(@_) unless $self->{quiet};
    };
  }
  return $self;
}


sub email_to {
  my ($self, @emails) = @_;
  my $old = $$self{email_to};
  if(@emails) {
    $$self{email_to} = [@emails];
  }
  return $old;
}


sub m {
  my ($self,$m) = shift;
  my $fh = $self->{LOG};
  my $t = sprintf("%.3f", time());
  $self->{logsub}->($self, 'msg', undef, undef, $t, @_);
}


sub ms {
  my $self = shift;
  $self->m(@_);
  $self->m($self->stack());
}


sub p {
  my ($self) = shift;
  my $fh = \*STDIN;
  my $regex = qr/.*/;
  my $default = undef;
  my @prompt = ();
  if(ref($_[0]) eq 'GLOB') {
    $fh = shift;
  }
  if(ref($_[-1]) eq 'Regexp') {
    $regex = pop;
  }
  elsif(ref($_[-2]) eq 'Regexp') {
    $default = pop;
    $regex = pop;
  }
  @prompt = @_;
  $self->m(@prompt);
  chomp($_ = <$fh>);
  if($default and $_ eq '') {
    $self->m('Using default:', $default);
    return $default;
  }
  while($_ !~ $regex) {
    $self->d("Input doesn't match:", $regex);
    $self->m(@prompt);
    chomp($_ = <$fh>);
  }

  $self->m('Using input:', $_);
  return $_;
}


sub e {
  my ($self,$m) = shift;
  my ($package, undef, $line) = caller 0;
  my $fh = $self->{LOG};
  my $t = sprintf("%.3f", time());
  $self->{logsub}->($self, 'err', $package, $line, $t, @_);
}


sub ed {
  my ($self) = shift;
  $self->e(@_);
  die(shift(@_) . "\n");
}


sub es {
  my $self = shift;
  $self->e(@_);
  $self->e($self->stack());
}


sub i {
  my $self = shift;
  my $fh = $self->{LOG};
  my $t = sprintf("%.3f", time());
  $self->{logsub}->($self, 'ifo', undef, undef, $t, @_);
}


sub is {
  my $self = shift;
  $self->i(@_);
  $self->i($self->stack());
}


sub d {
  my $self = shift;
  my ($package, undef, $line) = caller 0;
  my $fh = $self->{LOG};
  if(_PdbDEBUG) {
    my $t = sprintf("%.3f", time());
    $self->{logsub}->($self, 'dbg', $package, $line, $t, @_);
  }
}


sub ds {
  my $self = shift;
  $self->d(@_);
  $self->d($self->stack());
}


sub x {
  my ($self, $subref, @args) = @_;
  my $r = undef;
  my $saved_fhs = undef;
  my $proc_fh = undef;
  eval {
    $saved_fhs = $self->_save_stdfhs();
    open($proc_fh, '+>', undef) or die("Unable to open anonymous tempfile");
    open(STDOUT, '>&', $proc_fh) or die("Unable to dup anon fh to STDOUT");
    open(STDERR, '>&', \*STDOUT) or die("Unable to dup STDOUT to STDERR");
    $r = $subref->(@args);
  };
  $self->_restore_stdfhs($saved_fhs);
  seek($proc_fh, 0, SEEK_SET);
  return {rcode => $r, error => $EVAL_ERROR . $self->stack, fh => $proc_fh};
}


sub stack {
  my ($self, $level) = @_;
  $level = $self->{stack_depth} ||= 10 unless($level);
  my $out = "";
  my $i=0;
  my ($package, $file, $line, $sub) = caller($i+2); # +2 hides ProcessLog from the stack trace.
  $i++;
  if($package) {
    $out .= "Stack trace:\n";
  }
  else {
    $out .= "No stack data available.\n";
  }
  while($package and $i < $level) {
    $out .= " "x$i . "$package  $file:$line  $sub\n";
    ($package, $file, $line, $sub) = caller($i+2);
    $i++;
  }
  chomp($out);
  $out;
}

sub _p {
  my $mode = shift;
  my $package = shift;
  my $line = shift;
  my $time = shift;
  my $prefix = "$mode";
  $prefix .= " ${package}:${line}" if(defined $package and defined $line);
  $prefix .= $time ? " $time: " : ": ";
  @_ = map { (my $temp = $_) =~ s/\n/\n$prefix/g; $temp; }
       map { defined $_ ? $_ : 'undef' } @_;
  $prefix. join(' ',@_). "\n";
}

sub _flush {
  my ($self) = @_;
  unless($self->{log_path} =~ /^syslog:/) {
    $self->{LOG}->flush;
  }
  1;
}

sub _save_stdfhs {
  my ($self) = @_;
  open my $stdout_save, ">&", \*STDOUT or die("Unable to dup stdout");
  open my $stderr_save, ">&", \*STDERR or die("Unable to dup stderr");
  return { o => $stdout_save, e => $stderr_save };
}

sub _restore_stdfhs {
  my ($self, $fhs) = @_;
  my $o = $fhs->{o};
  my $e = $fhs->{e};
  open STDOUT, ">&", $o;
  open STDERR, ">&", $e;
  return 1;
}


sub email_and_die {
  my ($self, $extra) = @_;
  $self->e("Mail sending not available. Install Mail::Send, or perl-MailTools on CentOS") and die("Cannot mail out") unless($mail_available);
  $self->failure_email($extra);
  die($extra);
}


sub failure_email {
  my ($self,$extra) = @_;
  $self->send_email("$self->{script_name} FAILED", $extra);
}

sub success_email {
  my ($self, $extra) = @_;

  $self->send_email("$self->{script_name} SUCCESS", $extra);
}

sub send_email {
  my ($self, $subj, $body, @extra_to) = @_;
  $body ||= "No additional message attached.";
  my @to;
  unless( $mail_available ) {
    $self->e("Mail sending not available. Install Mail::Send, or perl-MailTools on CentOS");
    return 0;
  }
  unless( defined $self->{email_to} || @extra_to ) {
    $self->e("Cannot send email with no addresses.");
    return 0;
  }
  @to = ( (ref($self->{email_to}) eq 'ARRAY' ? @{$self->{email_to}} : $self->{email_to}), @extra_to );

  my $msg = Mail::Send->new(Subject => $subj);
  $msg->to(@to);
  my $fh = $msg->open;
  print($fh "Message from ", $self->{script_name}, " on ", hostname(), "\n");
  print($fh "RUN ID: ", $self->{run_id}, "\n");
  print($fh "Logging to: ", ($self->{log_path} =~ /^syslog/ ?
                               $self->{log_path}
                                 : File::Spec->rel2abs($self->{log_path})),
        "\n\n");
  print($fh $body);
  print($fh "\n");

  $fh->close;
}


{
  no strict 'refs';
  no warnings 'once';
  *::PL = \(ProcessLog->new($0, '/dev/null'));
}


1;
# ###########################################################################
# End ProcessLog package
# ###########################################################################

# ###########################################################################
# Which package fb3b29095206245c761e7099703527cd7483ab5d
# ###########################################################################
package Which;
use strict;
use warnings FATAL => 'all';
use Carp;

sub which($) {
  my $cmd = shift;
  croak "No command to which specified" if(!$cmd);
  if( $cmd =~ /^\.?\// or $cmd =~ /\// ) {
    return $cmd if(-f $cmd and -x $cmd);
    return undef;
  }
  for(split(/:/, $ENV{'PATH'})) {
    return "$_/$cmd" if(-f "$_/$cmd" and -x "$_/$cmd");
  }
  return undef;
}

1;
# ###########################################################################
# End Which package
# ###########################################################################

# ###########################################################################
# IniFile package 781eb70eee887952666c5fba5e81818d1f5f512f
# ###########################################################################
package IniFile;
use strict;
use warnings FATAL => 'all';
use File::Glob;


sub read_config {
  my $file = shift;
  my %cfg;
  my $inif;
  unless(open $inif, "<$file") {
    return undef;
  }
  my $cur_sec = '';
  while(<$inif>) {
    chomp;
    next if(/^\s*(?:;|#)/);
    next if(/^$/);
    if(/^\s*\[(\w+)\]/) { # Group statement
      $cfg{$1} = {};
      $cur_sec = $1;
    }
    elsif(/^!(include(?:dir)?)\s+([^\0]+)/) { # include directives
      my $path = $2;
      my @files;
      if($1 eq 'includedir') {
        @files = glob($path . "/*.cnf");
      }
      else {
        @files = ($path);
      }
      for(@files) { _merge(\%cfg, {read_config($_)}); }
    }
    else { # options and flags
      my ($k, $v) = split(/=/, $_, 2);
      $k =~ s/\s+$//;
      $k =~ s/^\s+//;
      if(defined($v)) {
        $v =~ s/^\s+//;
        $v =~ s/\s?#.*?[^"']$//;
        $v =~ s/^(?:"|')//;
        $v =~ s/(?:"|')$//;
      }
      else {
        if($k =~ /^(?:no-|skip-)(.*)/) {
          $k = $1;
          $v = 0;
        }
        else {
          $v = 1;
        }
      }
      chomp($k); chomp($v);

      if($k =~ /^(.*?)\s*\[\s*\d+\s*\]/) {
        $k = $1;
        push @{$cfg{$cur_sec}{$k}}, $v;
        next;
      }
      $cfg{$cur_sec}{$k} = $v;
    }
  }
  return %cfg;
}

sub _merge {
  my ($h1, $h2, $p) = @_;
  foreach my $k (keys %$h2) {
    if(not $p and not exists $h1->{$k}) {
      $h1->{$k} = $h2->{$k};
    }
    elsif(not $p and exists $h1->{$k}) {
      _merge($h1->{$k}, $h2->{$k}, $h1);
    }
    elsif($p) {
      $h1->{$k} = $h2->{$k};
    }
  }
  $h1;
}

1;
# ###########################################################################
# End IniFile package
# ###########################################################################

package XtraBackupAgent;
use strict;
use warnings FATAL => 'all';
use File::Path;
use File::Basename;
use File::Temp;
use IO::Select;
use IO::Handle;
use Sys::Hostname;
use MIME::Base64;

use POSIX;
use Tie::File;
use Fcntl qw(:flock);
use Data::Dumper;
use DBI;

{
  no warnings 'once';
  $Data::Dumper::Indent = 0;
  $Data::Dumper::Sortkeys = 1;
}

# client supplied header data
my %HDR = ();

# Default location for xtrabackup-agent to place any temporary files necessary.
my $GLOBAL_TMPDIR = "/tmp";

delete @ENV{'IFS', 'CDPATH', 'ENV', 'BASH_ENV'};
$ENV{PATH}="/usr/local/bin:/opt/csw/bin:/usr/bin:/usr/sbin:/bin:/sbin";
my $TAR = "tar";
my $TAR_WRITE_OPTIONS = "";
my $TAR_READ_OPTIONS = "";

# For testing purposes, it's nice to be able to have the agent
# communicate over alternate filehandles.
# Normally these refer to STDIN and STDOUT, respectively.
my $Input_FH;
my $Output_FH;

my $tmp_directory;
my $action;

our $VERSION="0.76.1";
my $REMOTE_VERSION = undef;
my $MIN_XTRA_VERSION=1.0;
my $XTRABACKUP_VERSION;

my $Log_Dir = $ENV{LOG_PATH} || "/var/log/mysql-zrm";
my $logFile = "$Log_Dir/xtrabackup-agent.log";

# Set to 1 inside the SIGPIPE handler so that we can cleanup innobackupex gracefully.
my $Stop_Copy = 0;
$SIG{'PIPE'} = sub { &printLog( "caught broken pipe\n" ); $Stop_Copy = 1; };
$SIG{'TERM'} = sub { &printLog( "caught SIGTERM\n" ); $Stop_Copy = 1; };

## Catch all errors and log them.
$SIG{'__DIE__'} = sub { die(@_) if($^S); $::PL->e(@_); die(@_); };

if($^O eq "linux") {
  $TAR_WRITE_OPTIONS = "--same-owner -cphsC";
  $TAR_READ_OPTIONS = "--same-owner -xphsC";
}
elsif($^O eq "freebsd" or $^O eq "darwin") {
  $TAR_WRITE_OPTIONS = " -cph -f - -C";
  $TAR_READ_OPTIONS = " -xp -f - -C";
}
else {
  # Assume GNU compatible tar
  $TAR_WRITE_OPTIONS = "--same-owner -cphsC";
  $TAR_READ_OPTIONS = "--same-owner -xphsC";
}

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

sub my_exit {
  ## Normally we always want to clean the temporary directory.
  ## In certain debugging situations we might want to inspect it's
  ## contents, which is why the following parameter exists.
  if( $tmp_directory and $HDR{'xtrabackup-agent:clean-tmpdir'} ){
    rmtree $tmp_directory, 0, 0;
  }
  exit( $_[0] );
}

sub printLog {
  my @args = @_;
  $::PL->m(@args);
}

sub printAndDie {
  my @args = @_;
  chomp(@args);
  $::PL->e(@args);
  printToServer("FAILED", join(' ', @args));
  my_exit( 1 );
}

sub getTmpName {
  if( ! -d $GLOBAL_TMPDIR ){
    printAndDie( "$GLOBAL_TMPDIR not found. Please create this first.\n" );
  }
  printLog( "TMP directory being used is $GLOBAL_TMPDIR\n" );
  return File::Temp::tempnam( $GLOBAL_TMPDIR, "" );
}

sub printToServer {
  my ($status, $msg) = @_;
  $msg =~ s/\n/\\n/g;
  print $Output_FH makeKvBlock(status => $status, msg => $msg);
}

# Compares version numbers in a pseudo-semversioning sort of way.
# Major revision changes are always incompatible.
# Minor revision changes are only compatible if the server
# is more new than the client.
# Revision changes should not affect compatibility.
# They exist to provide specific work arounds, if needed.
sub isClientCompatible {
  # Local Major/Minor/Revision parts.
  my ($L_Maj, $L_Min, $L_Rev) = split(/\./, $VERSION);
  my ($R_Maj, $R_Min, $R_Rev) = split(/\./, $REMOTE_VERSION);
  return 0 if($L_Maj != $R_Maj);
  return 1 if($L_Min >= $R_Min);
  return 0;
}

sub isLegacyClient {
  return $REMOTE_VERSION eq "1.8b7_palomino";
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
    printAndDie("Bad input:", $N);
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
  my $out = scalar(keys %Kv). "\n";
  foreach my $k (keys %Kv) {
    $out .= "$k=". (defined $Kv{$k} ? $Kv{$k} : '') . "\n";
  }
  $out .= "\n";
  $::PL->d('KvBlock:', $out);
  return $out;
}

# The header is composed of newline delimited data.
# Starting with version 0.75.1, the format is as follows:
#
#   <client version>\n
#   <key=value block>
#
# See readKvBlock() for format details of <key=value block>.
#
# When the server has read and validated the key=value block,
# it replies with 'READY'.
#
sub getHeader {
  $REMOTE_VERSION = <$Input_FH>;
  chomp($REMOTE_VERSION);
  $REMOTE_VERSION = checkIfTainted($REMOTE_VERSION);

  if(!isClientCompatible()) {
    printAndDie("Incompatible client version $REMOTE_VERSION.");
  }
  %HDR = %{readKvBlock(\*$Input_FH)};
  unless(exists $HDR{'action'}) {
    printAndDie("Missing required header key 'action'.");
  }
  $action = $HDR{'action'};
  $::PL->d('Request header:', Dumper(\%HDR));
  print $Output_FH "READY\n";
}

sub set_mysql_timeouts {
  my ($wait_timeout, $net_read_timeout, $net_write_timeout) = @_;
  $net_write_timeout ||= $net_read_timeout;
  $::PL->d('Setting timeouts (wait net_read net_write):',
           $wait_timeout, $net_read_timeout, $net_write_timeout);
  my @r;
  eval {
    my $dbh = get_dbh();
    $_ = $dbh->selectall_hashref("SHOW GLOBAL VARIABLES LIKE '%timeout'", 'Variable_name');
    $::PL->d('SQL: SHOW GLOBAL VARIABLES LIKE', "'%timeout'", 'Result:', Dumper($_));
    $r[0] = $_->{'wait_timeout'}->{'Value'};
    $r[1] = $_->{'net_read_timeout'}->{'Value'};
    $r[2] = $_->{'net_write_timeout'}->{'Value'};
    $dbh->do(
      qq{SET GLOBAL wait_timeout=$wait_timeout,
                    net_read_timeout=$net_read_timeout,
                    net_write_timeout=$net_write_timeout}
    );
  };
  if($@) {
    $_ = "$@";
    if($HDR{'xtrabackup-agent:must-set-mysql-timeouts'}) {
      printAndDie("ERROR", $_);
    }
    else {
      $::PL->e($_);
    }
  }
  $::PL->d('Original timeouts (wait net_read net_write):', @r);
  return @r;
}

sub get_dbh {
  my $socket = $HDR{'xtrabackup-agent:socket'};
  my $dbh = DBI->connect_cached(
    "DBI:mysql:host=localhost". ($socket ? ";mysql_socket=$socket" : ""),
    $HDR{'user'}, $HDR{'password'},
    { RaiseError => 1, AutoCommit => 0, PrintError => 0,
      ShowErrorStatement => 1});
  return $dbh;
}

## Provides the older, but compatible, uuencoded tar packing.
## This encoding is nearly 400% slower than the base64 encoding,
## but we need to keep it around for compatibility reasons.
sub _enc_uuencode {
  my ($fh) = @_;
  my $raw_sz = read($fh, $_, 10240);
  my $x = pack( "u*", $_ );

  return ($raw_sz, length($x), pack("N", length($x)), $x);
}

## Newer Base64 encoding. It's faster and more robust.
sub _enc_base64 {
  my ($fh) = @_;
  my $raw_sz = read($fh, $_, 180*57);
  my $x = encode_base64($_);
  return ($raw_sz, length($x), $x);
}

## NULL encoding - this will be fastest, but may not be suitable for use.
sub _enc_null {
  my ($fh) = @_;
  my $raw_sz = read($fh, $_, 10240);
  return ($raw_sz, $raw_sz, $_);
}

## Generic passthrough function to encode the stream
## as requested by the client. It defaults to uuencoding which,
## historically was the only encoding option. This enables seemless
## backwards compatibility with older client versions.
sub encode {
  my ($fh) = @_;
  if(not exists $HDR{'agent-stream-encoding'}
      or $HDR{'agent-stream-encoding'} eq 'application/x-uuencode-stream') {
    return _enc_uuencode($fh);
  }
  elsif($HDR{'agent-stream-encoding'} eq 'application/x-base64-stream') {
    return _enc_base64($fh);
  }
  elsif($HDR{'agent-stream-encoding'} eq 'application/octet-stream') {
    return _enc_null($fh);
  }
  else {
    printAndDie("Unknown encoding requested: $HDR{'agent-stream-encoding'}");
  }
}

sub do_innobackupex {
  my ($tmp_directory, %cfg) = @_;
  my ($fhs, $buf, $dbh, @timeouts, @cmd);

  my ($start_tm, $backup_sz) = (time(), 0);
  record_backup("full", $start_tm);

  if($Stop_Copy == 1) {
    # It's possible we could be interrupted before ever getting here.
    # Catch this.
    return;
  }

  @timeouts = set_mysql_timeouts(
    $cfg{'xtrabackup-agent:mysql-wait-timeout'},
    $cfg{'xtrabackup-agent:mysql-net-timeout'}
  );

  POSIX::mkfifo("/tmp/innobackupex-log", 0700);
  printLog("Created FIFOS..\n");

  ## Build our command.
  ##
  ## xtrabackup version 1.4 and (presently) greater have a bug, where
  ## it attempts to write to the files 'stderr' and 'stdout' in the
  ## current working directory. Since our normal working directory is
  ## the root directory, we now cd into a temporary directory before
  ## running innobackupex. This prevents being unable to write files.
  ##
  push(@cmd, "cd $tmp_directory;", $cfg{'xtrabackup-agent:innobackupex-path'});
  push(@cmd, "--user=$cfg{'user'}", "--password=$cfg{'password'}",
             "--defaults-file",
             $cfg{'xtrabackup-agent:my.cnf-path'},
             $cfg{'xtrabackup-agent:innobackupex-opts'},
             "--slave-info", "--stream=tar", $tmp_directory,
             "2>/tmp/innobackupex-log");

  if($cfg{'xtrabackup-agent:inline-compress'}) {
    $_ = $cfg{'xtrabackup-agent:inline-compress'};
    $::PL->d('Using inline compression program: ', $_);
    push(@cmd, "| $_");
  }
  push(@cmd, "|");

  ## Prepare to execute.
  $::PL->d("Exec:", @cmd);
  open(INNO_TAR, join(' ', @cmd));
  printLog("Opened InnoBackupEX.\n");
  open(INNO_LOG, "</tmp/innobackupex-log");
  printLog("Opened Inno-Log.\n");
  $fhs = IO::Select->new();
  $fhs->add(\*INNO_TAR);
  $fhs->add(\*INNO_LOG);
  $SIG{'PIPE'} = sub { printLog( "caught broken pipe\n" ); $Stop_Copy = 1; };
  $SIG{'TERM'} = sub { printLog( "caught SIGTERM\n" ); $Stop_Copy = 1; };
  while( $fhs->count() > 0 ) {
    if($Stop_Copy == 1) {
      set_mysql_timeouts(@timeouts);
      printLog("Copy aborted. Closing innobackupex.\n");
      $fhs->remove(\*INNO_TAR);
      $fhs->remove(\*INNO_LOG);
      close(INNO_TAR);
      close(INNO_LOG);
      printLog("Copy aborted. Closed innobackupex.\n");
      sendNagiosAlert("WARNING: Copy was interrupted!", 1);
      unlink("/tmp/innobackupex-log");
      record_backup("full", $start_tm, time(), $backup_sz, "failure", "copy interrupted");
      printAndDie("ERROR", "Finished cleaning up. Bailing out!\n");
    }
    my @r = $fhs->can_read(5);
    foreach my $fh (@r) {
      if($fh == \*INNO_LOG) {
        if( sysread( INNO_LOG, $buf, 1024 ) ) {
          printLog($buf);
          if($buf =~ /innobackupex.*: Error:(.*)/ || $buf =~ /Pipe to mysql child process broken:(.*)/) {
            record_backup("full", $start_tm, time(), $backup_sz, "failure", $1);
            set_mysql_timeouts(@timeouts);
            sendNagiosAlert("CRITICAL: $1", 2);
            unlink("/tmp/innobackupex-log");
            printAndDie($1);
          }
        }
        else {
          printLog("closed log handle\n");
          $fhs->remove($fh);
          close(INNO_LOG);
        }
      }
      if($fh == \*INNO_TAR) {
        my ($raw_sz, $packed_sz, @d) = encode(\*INNO_TAR);
        if( $raw_sz ) {
          print($Output_FH @d);
        }
        else {
          printLog("closed tar handle\n");
          $fhs->remove($fh);
          close(INNO_TAR);
          if($^O eq "freebsd") {
            printLog("closed log handle\n");
            $fhs->remove(\*INNO_LOG);
            close(INNO_LOG);
          }
        }
      }
    }
  }
  unlink("/tmp/innobackupex-log");
  set_mysql_timeouts(@timeouts);

  record_backup("full", $start_tm, time(), $backup_sz, "success");
  sendNagiosAlert("OK: Copied data successfully.", 0);
}

sub sendNagiosAlert {
  my $alert = shift;
  my $status = shift;
  my $host = hostname;
  my $nagios_service = $HDR{'xtrabackup-agent:nagios-service'};
  my $nagios_host    = $HDR{'xtrabackup-agent:nagios-host'};
  my $nsca_client    = $HDR{'xtrabackup-agent:send_nsca-path'};
  my $nsca_cfg       = $HDR{'xtrabackup-agent:send_nsca-cfg'};
  $status =~ s/'/\\'/g; # Single quotes are bad in this case.
  if($nagios_host) {
    printLog("Pinging nagios with: echo -e '$host\t$nagios_service\\t$status\\t$alert' | $nsca_client -c $nsca_cfg -H $nagios_host\n");
    $_ = qx/echo -e '$host\\t$nagios_service\\t$status\\t$alert' | $nsca_client -c $nsca_cfg -H $nagios_host/;
  }
}

#$_[0] dirname
#$_[1] filename
sub writeTarStream {
  my @cmd;
  my ($stream_from, $file, %cfg) = @_;
  my ($start_tm, $backup_sz) = (time(), 0);
  my $fileList = $file;
  my $lsCmd = "";
  my $tar_fh;

  my $tmpFile = getTmpName();

  if( $_[1] =~ /\*/) {
    $lsCmd = "cd $stream_from; ls -1 $file > $tmpFile 2>/dev/null;";
    my $r = system( $lsCmd );
    $fileList = " -T $tmpFile";
  }

  ## Build our command.
  ## Yes, stderr is ignored.
  push(@cmd, $TAR, $TAR_WRITE_OPTIONS, $stream_from, $fileList, '2>/dev/null');
  if($cfg{'xtrabackup-agent:inline-compress'}) {
    $_ = $cfg{'xtrabackup-agent:inline-compress'};
    $::PL->d('Using inline compression program: ', $_);
    push(@cmd, "| $_");
  }
  push(@cmd, '|');

  $::PL->d('Exec:', @cmd);
  if(!open( $tar_fh, join(' ', @cmd))) {
    record_backup("incremental", $start_tm, time(), $backup_sz, "failure", "$!");
    printAndDie( "tar failed $!\n" );
  }
  binmode($tar_fh);
  my ($raw_sz, $packed_sz, @x) = encode($tar_fh);
  while($raw_sz) {
    print($Output_FH @x);
    last if($Stop_Copy);
    ($raw_sz, $packed_sz, @x) = encode($tar_fh);
  }
  close( $tar_fh );
  printLog("tar exitval:", ($? >> 8));
  if(($? >> 8) == 2) {
    record_backup("incremental", $start_tm, time(), $backup_sz, "failure", "no such file/directory: $fileList");
    if( $lsCmd ){
      unlink( $tmpFile );
    }
    printAndDie("no such file(s) or director(ies): $fileList");
  }
  elsif(($? >> 8) > 0) {
    record_backup("incremental", $start_tm, time(), $backup_sz, "failure", "unknown failure retrieving: $fileList");
    if( $lsCmd ){
      unlink( $tmpFile );
    }
    printAndDie("unknown failure retrieving: $fileList");
  }

  if( $lsCmd ){
    unlink( $tmpFile );
  }
  record_backup("incremental", $start_tm, time(), $backup_sz, "success", $fileList);
}

sub open_stats_db {
  my $stats_db = $HDR{'xtrabackup-agent:stats-db-path'};
  my $do_lock = shift || LOCK_EX;
  my (@all_stats, $i) = ((), 0);
  my $st = tie @all_stats, 'Tie::File', $stats_db or printAndDie("ERROR", "unable to open the stats database $stats_db");
  if($do_lock) {
    for(1...3) {
      eval {
        local $SIG{ALRM} = sub { die('ALARM'); };
        alarm(5);
        $st->flock($do_lock);
        alarm(0);
      };
      if($@ and $@ =~ /ALARM/) {
        $::PL->e("on attempt", $_, "unable to flock $stats_db after 5 seconds.");
      }
      else {
        undef($st);
        return \@all_stats;
      }
    }
  }
  undef($st);
  untie(@all_stats);
  return undef;
}

sub record_backup {
  my ($type, $start_tm, $end_tm, $sz, $status, $info) = @_;
  my $stats_db = $HDR{'xtrabackup-agent:stats-db-path'};
  my ($all_stats, $i, $upd) = (undef, 0, 0);
  my $cnt = 0;
  if(not defined $type or not defined $start_tm) {
    die("Programming error. record_backup() needs at least two parameters.");
  }
  $end_tm = '-' if(not defined $end_tm);
  $sz = '-' if(not defined $sz);
  $status = $$ if(not defined $status);
  $info = '-' if(not defined $info);

  $all_stats = open_stats_db(LOCK_EX);
  if(not defined $all_stats) {
    untie(@$all_stats);
    printAndDie("ERROR", "unable to get an exclusive lock on the stats db $stats_db");
  }

  for($i = 0; $i < @$all_stats; $i++) {
    my $stat = $$all_stats[$i];
    next if($stat =~ /^$/);
    my ($stype, $sstart, $send, $ssize, $sstatus, $sinfo) = split(/\t/, $stat);
    if(!$upd and $stype eq $type and $start_tm == $sstart) {
      $$all_stats[$i] = join("\t", $type, $start_tm, $end_tm, $sz, $status, $info);
      $upd = 1;
      next;
    }
    if($stype eq $type) {
      if($cnt > $HDR{'xtrabackup-agent:stats-history'}) {
        delete $$all_stats[$i];
      }
      else {
        $cnt++;
      }
    }
  }
  unless($upd) {
    unshift @$all_stats, join("\t", $type, $start_tm, $end_tm, $sz, $status, $info);
  }
  untie(@$all_stats);
}

sub doMonitor {
  my ($newer_than, $max_items) = (0, 0);
  my ($all_stats, $i) = (undef, 0);
  my $stats_db = $HDR{'xtrabackup-agent:stats-db-path'};
  $newer_than = $HDR{newer_than};
  $max_items = $HDR{max_items};

  $all_stats = open_stats_db(LOCK_SH);
  if(not defined $all_stats) {
    untie(@$all_stats);
    printAndDie("ERROR", "unable to get a lock on the stats db $stats_db");
  }

  foreach my $stat (@$all_stats) {
    my ($stype, $sstart, $send, $ssize, $sstatus, $info) = split(/\t/, $stat);
    if($sstart >= $newer_than) {
      print($Output_FH $stat, "\n");
      $i++;
    }
    if($i == $max_items) {
      last;
    }
  }
  untie(@$all_stats);
}

sub checkXtraBackupVersion {
  # xtrabackup  Ver 0.9 Rev 83 for 5.0.84 unknown-linux-gnu (x86_64)
  eval {
    unless(Which::which('xtrabackup') and Which::which('innobackupex-1.5.1')) {
      printAndDie("ERROR", "xtrabackup is not properly installed, or not in \$PATH.");
    }
    $_ = qx/xtrabackup --version 2>&1/;
    if(/^xtrabackup\s+Ver\s+(\d+\.\d+)/) {
      $XTRABACKUP_VERSION=$1;
      if($MIN_XTRA_VERSION > $XTRABACKUP_VERSION) {
        printAndDie("ERROR", "xtrabackup is not of the minimum required version: $MIN_XTRA_VERSION > $XTRABACKUP_VERSION.");
      }
    }
    else {
      printAndDie("ERROR", "xtrabackup did not return a valid version string");
    }
  };
  if($@) {
    chomp($@);
    printAndDie("ERROR", "xtrabackup not present or otherwise not executable. $@");
  }
}

sub processRequest {
  ($Input_FH, $Output_FH, $logFile) = @_;
  my $dbh;

  $::PL->logpath($logFile);
  $::PL->quiet(1);

  printLog("Server($VERSION) started.");
  $::PL->d("Server enivronment:", Dumper(\%ENV));
  $Input_FH->autoflush(1);
  $Output_FH->autoflush(1);
  getHeader();
  printLog("Client $ENV{'REMOTE_HOST'} ($REMOTE_VERSION) connected." );

  checkXtraBackupVersion();

  ## Set default values.
  $HDR{'xtrabackup-agent:stats-db-path'} ||= "$Log_Dir/stats.db";
  $HDR{'xtrabackup-agent:stats-history'} ||= 1000;
  $HDR{'xtrabackup-agent:innobackupex-path'} ||= Which::which('innobackupex-1.5.1');
  $HDR{'xtrabackup-agent:innobackupex-opts'} ||= "";
  $HDR{'xtrabackup-agent:my.cnf-path'} ||= "/etc/my.cnf";
  $HDR{'xtrabackup-agent:perl-lib-extra'} ||= "";
  $HDR{'xtrabackup-agent:mysql-install-path'} ||= "";
  $HDR{'xtrabackup-agent:mysql-wait-timeout'} ||= 8*3600;
  $HDR{'xtrabackup-agent:mysql-net-timeout'}  ||= 8*3600;
  $HDR{'xtrabackup-client:nagios-host'} ||= "";
  $HDR{'xtrabackup-client:nagios-service'} ||= "MySQL Backups";
  $HDR{'xtrabackup-client:send_nsca-path'} ||= Which::which("send_nsca");
  $HDR{'xtrabackup-client:send_nsca-config'} ||= "/usr/share/mysql-zrm/plugins/nsca.cfg";

  if(not exists $HDR{'xtrabackup-agent:must-set-mysql-timeouts'}) {
    $HDR{'xtrabackup-agent:must-set-mysql-timeouts'} = 1;
  }

  $::PL->d('Adjusted Header:', Dumper(\%HDR));

  eval {
    $dbh = get_dbh();
  };
  if( $@ ) {
    $_ = "$@";
    record_backup($HDR{'backup-level'} ? "full" : "incremental", time(), time(), '-', "failure", $_);
    printAndDie("ERROR", "Unable to open DBI handle.", "Error:", $_);
  }

  if($action eq "copy from") {
    if(not exists $HDR{'backup-level'} or not exists $HDR{'user'}
        or not exists $HDR{'password'} or not exists $HDR{'file'}) {
      printAndDie("Mandatory parameters missing: user, password, backup-level, file.")
    }
    ##
    ## ZRM defines two kinds of backups: full and incremental.
    ## In the configuration, a full backup is denoted by setting
    ## backup-level=0 and an incremental by backup-level=1.
    ##
    ## Below is the handling code for a full backup.
    ##
    if($HDR{'backup-level'} == 0) {
      if($HDR{'file'} =~ /ZRM_LINKS/) {
        if($HDR{'replication'} == 1) {
          ## Re-enable replication.
          $dbh->do('START SLAVE');
        }
        eval {
          $tmp_directory=getTmpName();
          $::PL->d("Setting up temporary directory:", $tmp_directory);
          mkdir($tmp_directory);
          print($Output_FH makeKvBlock('status' => 'SENDING'));
          do_innobackupex($tmp_directory, %HDR);
        };
        if($@) {
          printAndDie("full backup failed in a new and unusual way: $@");
        }
      }
      else {
        $::PL->m('Ignored duplicate/extra/useless request for:', $HDR{'file'});
        print($Output_FH makeKvBlock('status' => 'OK'));
      }
    }
    ##
    ## Following this comment is the code for handling incremental backups.
    ##
    elsif($HDR{'backup-level'} == 1) { # An incremental backup.
      my $fh;
      my $last_sid = undef;
      eval {
        open($fh, "<$Log_Dir/incremental.sid") or die("$!\n");
        chomp($last_sid = <$fh>);
        close($fh);
      };

      if(not defined($last_sid) or $last_sid ne $HDR{'sid'}) {
        my $slave_status = {};
        my $master_logs  = [];
        my $next_binlog;

        $master_logs  = $dbh->selectall_arrayref('SHOW MASTER LOGS', { Slice => {} });

        ## These will only return useful information when replication=1 anyway.
        ## So there is little to no point in sending this information along
        ## if it will only be confusing and misleading.
        if($HDR{'replication'} == 1) {
          $slave_status = $dbh->selectrow_hashref('SHOW SLAVE STATUS', { Slice => {} });
          $dbh->do('START SLAVE');
        }

        my ($file, $dir, $suffix) = fileparse($HDR{'file'});
        if($HDR{'mysql-binlog-path'}) { # Someone has put the binlogs somewhere else..
          $::PL->d('Overriding binlog directory with:', $HDR{'mysql-binlog-path'});
          $dir = $HDR{'mysql-binlog-path'};
        }
        $master_logs = [ map { $_->{'Log_name'} } @$master_logs ];
        while( ($_ = shift @$master_logs ) ne $HDR{'binlog'}) {}
        unshift @$master_logs, $HDR{'binlog'};
        $next_binlog = pop @$master_logs;
        $::PL->d('Copying binlogs: ', @$master_logs);
        $::PL->d('Slave status: ', Dumper($slave_status));
        print($Output_FH makeKvBlock(%$slave_status, 'status' => 'SENDING'));
        writeTarStream( $dir, join(' ', @$master_logs), %HDR);

        open($fh, ">$Log_Dir/incremental.sid");
        print($fh "$HDR{'sid'}\n");
        close($fh);

      }
      else {
        $::PL->m('Ignored duplicate/extra/useless request for:', $HDR{'file'});
        print($Output_FH makeKvBlock('status' => 'OK'));
      }
    }
  }
  elsif($action eq "monitor") {
    doMonitor();
  }
  else {
    $::PL->i("Unknown action: $action, ignoring.");
  }
  printLog( "Server exit" );
  my_exit( 0 );
}

# if someone didn't "require xtrabackup-agent.pl" us, then
# we can assume we're supposed to process a request and exit.
if(!caller) { processRequest(\*STDIN, \*STDOUT, $logFile); }

1;

