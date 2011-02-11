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

my $INNOBACKUPEX="innobackupex-1.5.1";

our $VERSION="0.75.1";
my $REMOTE_VERSION = undef;
my $MIN_XTRA_VERSION=1.0;
my $XTRABACKUP_VERSION;

my $logDir = $ENV{LOG_PATH} || "/var/log/mysql-zrm";
my $logFile = "$logDir/xtrabackup-agent.log";
my $snapshotInstallPath = "/usr/share/mysql-zrm/plugins";

# Set to 1 inside the SIGPIPE handler so that we can cleanup innobackupex gracefully.
my $Stop_Copy = 0;
$SIG{'PIPE'} = sub { &printLog( "caught broken pipe\n" ); $Stop_Copy = 1; };
$SIG{'TERM'} = sub { &printLog( "caught SIGTERM\n" ); $Stop_Copy = 1; };


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

## Catch all errors and log them.
$SIG{'__DIE__'} = sub { die(@_) if($^S); $::PL->e(@_); die(@_); };

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
  chomp(@args);
  $::PL->m(@args);
}

sub printAndDie {
  my @args = @_;
  chomp(@args);
  $::PL->e(@args);
  printToServer("FAILED", join(' ', @args));
  &my_exit( 1 );
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
  $::PL->d('Stream debug (Client Version):', "'$REMOTE_VERSION'");

  if(!isClientCompatible()) {
    printAndDie("Incompatible client version.");
  }
  %HDR = %{readKvBlock(\*$Input_FH)};
  unless(exists $HDR{'action'}) {
    printAndDie("Missing required header key 'action'.");
  }
  $action = $HDR{'action'};
  print $Output_FH "READY\n";
}

sub restore_wait_timeout {
  my ($dbh, $prev_wait) = @_;

  if($dbh and $prev_wait){
    printLog("Re-setting wait_timeout to $prev_wait\n");
    $dbh->do("SET GLOBAL wait_timeout=$prev_wait");
  }
  else {
    undef;
  }
  undef;
}

sub do_innobackupex {
  my ($tmp_directory, %cfg) = @_;
  my @cmd;
  my ($fhs, $buf, $dbh, $prev_wait);

  my ($start_tm, $backup_sz) = (time(), 0);
  record_backup("full", $start_tm);

  if($Stop_Copy == 1) {
    # It's possible we could be interrupted before ever getting here.
    # Catch this.
    return;
  }

  POSIX::mkfifo("/tmp/innobackupex-log", 0700);
  printLog("Created FIFOS..\n");

  eval {
    $dbh = DBI->connect("DBI:mysql:host=localhost". ($mysql_socket_path ? ";mysql_socket=$mysql_socket_path" : ""), $cfg{'user'}, $cfg{'password'}, { RaiseError => 1, AutoCommit => 1});
  };
  if($@) {
    printLog("Unable to open DBI handle. Error: $@");
    if($must_set_wait_timeout) {
      record_backup("full", $start_tm, time(), $backup_sz, "failure", "$@");
      printAndDie("ERROR", "Unable to open DBI handle. $@\n");
    }
  }

  if($dbh) {
    $prev_wait = $dbh->selectrow_arrayref("SHOW GLOBAL VARIABLES LIKE 'wait_timeout'")->[1];
    eval {
      $dbh->do("SET GLOBAL wait_timeout=$wait_timeout");
    };
    if( $@ ) {
      printLog("Unable to set wait_timeout. $@\n");
      if($must_set_wait_timeout) {
        record_backup("full", $start_tm, time(), $backup_sz, "failure", "unable to set wait_timeout");
        printAndDie("ERROR", "Unable to set wait_timeout. $@\n");
      }
    }
    printLog("Got db handle, set new wait_timeout=$wait_timeout, previous=$prev_wait\n");
  }

  ## Build our command.
  ##
  ## xtrabackup version 1.4 and (presently) greater have a bug, where
  ## it attempts to write to the files 'stderr' and 'stdout' in the
  ## current working directory. Since our normal working directory is
  ## the root directory, we now cd into a temporary directory before
  ## running innobackupex. This prevents being unable to write files.
  ##
  push(@cmd, "cd $tmp_directory;", $INNOBACKUPEX);
  push(@cmd, "--user=$cfg{'user'}", "--password=$cfg{'password'}",
             "--defaults-file", $mycnf_path, $innobackupex_opts,
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
      restore_wait_timeout($dbh, $prev_wait);
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
        if( sysread( INNO_LOG, $buf, 10240 ) ) {
          printLog($buf);
          if($buf =~ /innobackupex.*: Error:(.*)/ || $buf =~ /Pipe to mysql child process broken:(.*)/) {
            record_backup("full", $start_tm, time(), $backup_sz, "failure", $1);
            restore_wait_timeout($dbh, $prev_wait);
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
        if( sysread( INNO_TAR, $buf, 10240 ) ) {
          $backup_sz += length($buf);
          my $x = pack( "u*", $buf );
          print $Output_FH pack( "N", length( $x ) );
          print $Output_FH $x;
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
  printLog("Pinging nagios with: echo -e '$host\t$nagios_service\\t$status\\t$alert' | $nsca_client -c $nsca_cfg -H $nagios_host\n");
  $_ = qx/echo -e '$host\\t$nagios_service\\t$status\\t$alert' | $nsca_client -c $nsca_cfg -H $nagios_host/;
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
  my $buf;
  while( read( $tar_fh, $buf, 10240 ) ) {
    my $x = pack( "u*", $buf );
    $backup_sz += length($buf);
    print $Output_FH pack( "N", length( $x ) );
    print $Output_FH $x;
    last if($Stop_Copy);
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

sub open_stats_db {
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
      if($cnt > 30) {
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
      printToServer("ERROR", "xtrabackup is not properly installed, or not in \$PATH.");
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

  $::PL->logpath($logFile);
  $::PL->quiet(1);

  printLog("Server($VERSION) started.");
  $Input_FH->autoflush(1);
  $Output_FH->autoflush(1);
  getHeader();
  printLog("Client Version: $REMOTE_VERSION" );

  checkXtraBackupVersion();

  $::PL->d('Client Header:', Dumper(\%HDR));

  if($action eq "copy from") {
    if(not exists $HDR{'backup-level'} or not exists $HDR{'user'}
        or not exists $HDR{'password'}) {
      printAndDie("Mandatory parameters missing: user, password, backup-level.")
    }
    if($HDR{'backup-level'} == 0) { # A full backup.
      if($HDR{'file'} =~ /ZRM_LINKS/) {
        print($Output_FH makeKvBlock('status' => 'SENDING'));
        $tmp_directory=getTmpName();
        mkdir($tmp_directory);
        do_innobackupex($tmp_directory, %HDR);
      }
      else {
        $::PL->m('Ignored duplicate/extra/useless request for:', $HDR{'file'});
        print($Output_FH makeKvBlock('status' => 'OK'));
      }
    }
    elsif($HDR{'backup-level'} == 1) { # An incremental backup.
      my $fh;
      my $last_sid = undef;
      eval {
        open($fh, "<$logDir/incremental.sid") or die("$!\n");
        chomp($last_sid = <$fh>);
        close($fh);
      };

      if(not defined($last_sid) or $last_sid ne $HDR{'sid'}) {
        my $dbh;
        my $slave_status = {};
        my $master_logs  = {};
        my $next_binlog;
        eval {
          $dbh = DBI->connect("DBI:mysql:host=localhost".
            ($HDR{'socket'} ? ";mysql_socket=$HDR{'socket'}" : ""),
            $HDR{'user'}, $HDR{'password'}, { RaiseError => 1, AutoCommit => 1});
        };
        if( $@ ) {
          printLog("Unable to open DBI handle. Error: $@\n");
          record_backup("incremental", time(), time(), '-', "failure", "$@");
          printAndDie("ERROR", "Unable to open DBI handle. $@\n");
        }

        ## These will only return useful information when replication=1 anyway.
        ## So there is little to no point in sending this information along
        ## if it will only be confusing and misleading.
        if($HDR{'replication'} == 1) {
          $slave_status = $dbh->selectrow_hashref('SHOW SLAVE STATUS', { Slice => {} });
          $master_logs  = $dbh->selectall_arrayref('SHOW MASTER LOGS', { Slice => {} });
          $dbh->do('START SLAVE');
        }

        my ($file, $dir, $suffix) = fileparse($HDR{'file'});
        $master_logs = [ map { $_->{'Log_name'} } @$master_logs ];
        while( ($_ = shift @$master_logs ) ne $HDR{'binlog'}) {}
        unshift @$master_logs, $HDR{'binlog'};
        $next_binlog = pop @$master_logs;
        $::PL->d('Copying binlogs: ', @$master_logs);
        $::PL->d('Slave status: ', Dumper($slave_status));
        print($Output_FH makeKvBlock(%$slave_status, 'status' => 'SENDING'));
        writeTarStream( $dir, join(' ', @$master_logs), %HDR);

        open($fh, ">$logDir/incremental.sid");
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

