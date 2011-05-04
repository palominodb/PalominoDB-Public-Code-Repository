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

# ###########################################################################
# ZRMBackup package ced6cb18928cb013befc68164fd22b68695a7bca
# ###########################################################################
package ZRMBackup;
use strict;
use 5.008;
use warnings FATAL => 'all';

use English qw(-no_match_vars);
use Data::Dumper;
use File::Spec;


sub new {
  my ( $class, $pl, $backup_dir ) = @_;
  my $self = ();
  $self->{backup_dir} = ($backup_dir ? $backup_dir : $pl);
  bless $self, $class;

  unless( $self->_load_index() ) {
    return undef;
  }
  return $self;
}

sub DESTROY {}

sub backup_dir {
  my ($self) = @_;
  return $self->{backup_dir};
}

sub open_last_backup {
  my ($self) = @_;
  if(not defined $self->last_backup) {
    die("No last backup present.\n");
  }
  return ZRMBackup->new(undef, $self->last_backup);
};

sub find_full {
  my ($self, $strip, $rel_base) = @_;
  my @backups;
  unshift @backups, $self;
  while($backups[0] && $backups[0]->backup_level != 0) {
    $::PL->d("unadjusted lookup:", $backups[0]->last_backup);
    my @path = File::Spec->splitdir($backups[0]->last_backup);
    my $path;
    if($strip and $strip =~ /^\d+$/) {
      for(my $i=0; $i<$strip; $i++) { shift @path; }
    }
    elsif($strip) {
      $_ = $backups[0]->last_backup;
      s/^$strip//;
      @path = File::Spec->splitdir($_);
    }
    if($rel_base) {
      unshift @path, $rel_base;
    }
    $path = File::Spec->catdir(@path);
    $::PL->d("adjusted lookup:", $path);
    unshift @backups, ZRMBackup->new(undef, $path);
  }
  shift @backups unless($backups[0]);
  if($backups[0]->backup_level != 0) {
    croak('No full backup present in chain');
  }
  return @backups;
}

sub extract_to {
  my ($self, $xdir) = @_;
  my @args = ();
  if($self->compress =~ /gzip/) {
    @args = ("tar","-xzf", $self->backup_dir . "/backup-data", "-C", $xdir);
  }
  elsif($self->compress =~ /bzip2/) {
     @args = ("tar","-xjf", $self->backup_dir . "/backup-data", "-C", $xdir);
  }
  else {
    @args = ($self->compress ." -d ". $self->backup_dir ."/backup-data". " | tar -C $xdir -xf -");
  }
  my $r = $::PL->x(sub { system(@_) }, @args);
  return wantarray ? ($r->{rcode}, $r->{fh}) : $r->{rcode};
}


sub _load_index() {
  my ($self) = @_;
  my $fIdx;
  unless(open $fIdx, "<$self->{backup_dir}/index") {
    return undef;
  }
  $self->{idx} = ();
  while(<$fIdx>) {
    chomp;
    next if $_ eq ""; # Skip empty lines.
    my ($k, $v) = split(/=/, $_, 2);
    next if ($k eq "");
    $k =~ s/-/_/g;
    next if $k =~ /\//; # File lists are useless to us right now.
    if($k eq "backup_size" or $k eq "backup_size_compressed") {
      if($v =~ / MB$/) {
        $v =~ s/ MB$//;
        $v *= 1024;
      }
      elsif($v =~ / GB$/) {
        $v =~ s/ GB$//;
        $v *= 1024;
        $v *= 1024;
      }
    }
    elsif($k eq "backup_status") {
      if($v eq "Backup succeeded") {
        $v = 1;
      }
      else {
        $v = 0;
      }
    }
    elsif($k =~ /_time$/) {
      my ($h, $m, $s) = split(/:/, $v);
      $v  = $h*3600;
      $v += $m*60;
      $v += $s;
    }
    elsif($k eq "raw_databases_snapshot" or $k eq "replication") {
      my @t = split(/\s+/, $v);
      $v = \@t;
    }
    $self->{idx}{$k} = $v;
  }
  return 1;
}

our $AUTOLOAD;
sub AUTOLOAD {
  my ($self) = @_;
  my $name = $AUTOLOAD;
  $name =~ s/.*:://;
  ProcessLog::_PdbDEBUG >= ProcessLog::Level2
    && $::PL->d("AUTOLOAD:", $name, '->', $self->{idx}{$name});
  if(exists $self->{idx}{$name}) {
    return $self->{idx}{$name};
  }
  return undef;
};

1;
# ###########################################################################
# End ZRMBackup package
# ###########################################################################

package XtraBackupClient;

use strict;
use warnings FATAL => 'all';
use Socket;
use File::Temp qw/ :POSIX /;
use Getopt::Long qw(:config no_ignore_case);
use File::Basename;
use File::Spec::Functions;
use Data::Dumper;
use Text::ParseWords;
use MIME::Base64;
use POSIX qw(floor);

##
## Default size for reads from the network. (10k).
## This value should *never* be changed, as doing so could
## break compatibility with older agents.
##
use constant DEFAULT_BLOCK_SIZE => 10240;

{
  no warnings 'once';
  $Data::Dumper::Indent = 0;
  $Data::Dumper::Sortkeys = 1;
}

my $TAR = "tar";
my $TAR_WRITE_OPTIONS = "";
my $TAR_READ_OPTIONS = "";

my $VERSION = "0.75.1";
my $REMOTE_PORT=25300;

my %o;
my %c;

$SIG{'PIPE'} = sub { $::PL->end; die "Pipe broke"; };
$SIG{'TERM'} = sub { close SOCK; $::PL->end; die "TERM broke\n"; };

$SIG{'__DIE__'} = sub { die(@_) if($^S); $::PL->e(@_); $::PL->end; exit(1); };

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
  if($N !~ /^\d+$/) {
    printAndDie("Bad input: $_");
  }
  for($i = 0; $i < $N; $i++) {
    chomp($_ = <$fh>);
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
  $::PL->d('makeKvBlock:', Dumper(\%Kv));
  my $out = scalar(keys %Kv). "\n";
  foreach my $k (keys %Kv) {
    $out .= "$k=". (defined $Kv{$k} ? $Kv{$k} : '') . "\n";
  }
  $out .= "\n";
  $::PL->d('KvBlock:', $out);
  return $out;
}

sub agentWrite {
  my (%Args) = @_;
  print(SOCK makeKvBlock(%Args));
}

sub agentRead {
  return readKvBlock(\*SOCK);
}

sub agentRequest {
  my ($host, $port, $action, $sid, %params) = @_;
  $::PL->d('Attempting to connect to:', $host, 'port:', $port, "\n",
           "action:", $action, "sid:", $sid);
  my $iaddr = inet_aton($host) or die "no host: $host";
  my $paddr = sockaddr_in($port, $iaddr);
  my $proto = getprotobyname('tcp');
  socket(SOCK, PF_INET, SOCK_STREAM, $proto) or die "socket: $!";
  connect(SOCK, $paddr) or die "connect: $!";
  select( SOCK );
  $| = 1;
  select( STDOUT );
  $::PL->m("Connected to $host:$port.");

  my $tmp = File::Spec->tmpdir();
  my $args = makeKvBlock('action' => $action, 'tmpdir' => $tmp,
                          'sid' => $sid, %c, %params);
  print SOCK "$VERSION\n";
  print SOCK $args;
  $_ = <SOCK>;
  if(!/READY/) {
    printAndDie("Agent did not respond properly. Expected: READY, Got: $_");
  }
}

sub printAndDie {
  $::PL->ed(@_);
  $::PL->end;
  die("ERROR: @_\n");
}

sub my_exit {
  $::PL->end;
  exit($_[0]);
}

## Reads length prefixed uuencoded stream data.
## The structure is (in perl pack format): Nu
## Note: The N in this format is redundant
## because the server side has historically never sent more
## than 10240 bytes of uuencoded data.
## More recent versions can send much larger blocks.
sub read_uuencoded {
  my ($fh) = @_;
  my $buf;

  ## Read network packed byte count.
  ## If a false value is returned from read()
  ## then we return undef because no more data
  ## is available.
  if(!read($fh, $buf, 4)) {
    return undef;
  }
  $buf = unpack('N', $buf);
  # Incoming blocks should never be larger than this.
  # If we receive a block more than 38% of the requested block size
  # then we abort. The other hand has more than likely failed anyway.
  # The reason for 38% is that uuencoding adds roughly 37.8% to the
  # blocksize.
  if($buf > 1.38*$c{'xtrabackup-client:stream-block-size'}) {
    $::PL->e('Invalid block of size', $buf, 'when expected size is',
             $c{'xtrabackup-client:stream-block-size'});
    return undef;
  }
  read($fh, $buf, $buf);
  ProcessLog::_PdbDEBUG >= ProcessLog::Level3
  && $::PL->d('Read stream', '('. length($buf) .' bytes):', 'binary data');
  return unpack('u', $buf);
}

sub read_base64 {
  my ($fh) = @_;
  my $buf;
  read($fh, $buf, floor($c{'xtrabackup-client:stream-block-size'}/57)*77);
  ProcessLog::_PdbDEBUG >= ProcessLog::Level3
  && $::PL->d('Read stream', '('. length($buf) .' bytes):', $buf);
  return decode_base64($buf);
}

sub read_null {
  my ($fh) = @_;
  my $buf;
  read($fh, $buf, $c{'xtrabackup-client:stream-block-size'});
  ProcessLog::_PdbDEBUG >= ProcessLog::Level3
  && $::PL->d('Read stream', '('. length($buf) .' bytes):', 'binary data');
  return $buf;
}

sub decode {
  my ($fh) = @_;
  my $buf = '';
  for(my $i=0; $i<$c{'xtrabackup-client:network-failure-retry'}; $i++) {
    eval {
      local $SIG{'ALRM'} = sub { die("ALARM\n"); };
      alarm($c{'xtrabackup-client:network-timeout'});
      if(not exists $c{'xtrabackup-client:stream-encoding'}
          or $c{'xtrabackup-client:stream-encoding'} eq 'uuencode'
          or $c{'xtrabackup-client:stream-encoding'} eq 'default') {
        $buf = read_uuencoded($fh);
      }
      elsif($c{'xtrabackup-client:stream-encoding'} eq 'base64') {
        $buf = read_base64($fh);
      }
      elsif($c{'xtrabackup-client:stream-encoding'} eq 'null') {
        $buf = read_null($fh);
      }
      alarm(0);
    };
    if($@ and $@ =~ /ALARM/) {
      alarm(0);
      if($c{'xtrabackup-client:on-network-failure'} eq 'retry') {
        next;
      }
      elsif($c{'xtrabackup-client:on-network-failure'} eq 'abort') {
        die("Network timeout after $c{'xtrabackup-client:network-timeout'} seconds");
      }
    }
    return $buf;
  }
  die("Network timeout after $c{'xtrabackup-client:network-failure-retry'} tries");
}

# This will read the data from the socket and pipe the output to tar
sub readTarStream {
  my $buf;
  my $tar_fh;
  my $tmpfile = tmpnam();
  my $destDir = $o{'destination-directory'};
  my @cmd = ($TAR, $TAR_READ_OPTIONS, $destDir, "2>$tmpfile");

  if($c{'xtrabackup-agent:inline-compress'}) {
    $::PL->m("Incoming tar stream is compressed with:",
             $c{'xtrabackup-agent:inline-compress'});
    unshift(@cmd, $c{'xtrabackup-agent:inline-compress'}, '-d', '|');
  }
  unshift(@cmd, '|');

  ## We must check that the backup-level is 0 (full)
  ## because ZRM is MUCH more pedantic about incrementals.
  ## In the incremental case, it checks for the existance
  ## of every file it expects to receive and annoyingly
  ## flags the backup as "done, but with errors" if not
  ## all of them are present. This makes sysadmins unhappy.
  if($c{'backup-level'} == 0) {
    if($c{'xtrabackup-client:unpack-backup'} == 0) {
      $::PL->m("Not unpacking incoming tar stream.\n",
               "The data will be written directly to $destDir/backup-data.");
      my $backup_data_fh;
      if(!open($backup_data_fh, ">$destDir/backup-data")) {
        printAndDie("Unable to open $destDir/backup-data (". int($!) ."): $!");
      }
      binmode($backup_data_fh);
      while($_ = decode(\*SOCK)) {
        print($backup_data_fh $_);
      }
      if(!close($backup_data_fh)) {
        printAndDie("Unable to close backup-data file (". int($!) ."): $!")
      }
      return;
    }
  }

  $::PL->m("read-tar-stream:", @cmd);
  if(!open($tar_fh, join(' ', @cmd))) {
    printAndDie("tar failed $!");
  }
  binmode($tar_fh);

  while($_ = decode(\*SOCK)){
    ProcessLog::_PdbDEBUG >= ProcessLog::Level3
    && $::PL->d('Writing to tar:', length($_), 'bytes');
    print($tar_fh $_);
  }
  {
    local $/;
    open my $fh, "<$tmpfile";
    my $errs = <$fh>;
    chomp($errs);
    $::PL->e("tar-errors (may be empty): '". (defined($errs) ? $errs : '(undef)'). "'");
    close $fh;
    unless(exists $c{'xtrabackup-client:clean-tmpdir'}
           and $c{'xtrabackup-client:clean-tmpdir'}) {
      unlink $tmpfile;
    }
  }
  unless( close($tar_fh) ){
    printAndDie('tar pipe failed');
  }
}

# This just fiddles with the tar options to get it to read an innobackupex
# compatible tar stream, and optionally runs --apply-log, if requested by
# the xtrabackup-client:run-apply-log=1 option.
sub readInnoBackupStream {
  if( $c{'xtrabackup-client:tar-force-ownership'} == 0 ) {
    $TAR_READ_OPTIONS = "--no-same-owner --no-same-permissions -xiC";
  }
  else {
    $TAR_READ_OPTIONS = "--same-owner -xipC";
  }

  readTarStream();

  if( $c{'backup-level'} == 0 and $c{'xtrabackup-client:run-apply-log'} == 1 ) {
    if($c{'xtrabackup-client:unpack-backup'} == 0) {
      $::PL->i('The options xtrabackup-client:unpack-backup and xtrabackup-client:run-apply-log',
               'are mutually exclusive. Skipping apply log step.');
      return;
    }
    if(not defined $c{'xtrabackup-client:innobackupex-path'}) {
      $::PL->i("Unable to determine path to innobackupex-1.5.1.\n",
      'Check your PATH, or set xtrabackup-client:innobackupex-path in your config.');
    }
    else {
      $::PL->m("Applying logs..");
      my $r = $::PL->x(sub { system @_; },
          "cd $o{'destination-directory'} && ".
          "$c{'xtrabackup-client:innobackupex-path'} ".
          "--apply-log $o{'destination-directory'}"
          );
      my $fh = $$r{fh};
      while(<$fh>) { $::PL->m($_); }
      if($$r{rcode} != 0) {
        $::PL->e("Applying the innobackup logs failed.");
      }
      if($$r{error}) { printAndDie("Error executing innobackupex."); }
    }
  }
}

sub dummySnapshotDeviceInfo {
  my %snap_o;
  ## This annoying paradigm brought to you by ZRM.
  ## It does a slight of hand when dealing with "snapshot plugins",
  ## When the snapshot plugin is called on the backup server machine, the
  ## module ZRM::SnapshotCommon detects this and transparently turns it into
  ## a call to the copy plugin and passes the arguments destined for the
  ## snapshot plugin on the remote end as a single string to the flag
  ## '--snapshot-parameters'. Since I don't feel like doing a regex just to
  ## pull out the '--action' flag, I save and restore @ARGV and call
  ## GetOptions().
  ##
  ## If the action is 'get-vm-device-details' then just *print* a dummy
  ## block of text for use to ZRM. ZRM catches this information and normally
  ## does some innane thing to it, since we don't actually *make* a snapshot,
  ## we can return any old thing - the values here are selected to be as
  ## impossible as can be.
  ##
  ## All other actions are ignored.

  my @ARGS_SAVE = @ARGV;
  @ARGV = shellwords($o{'snapshot-parameters'});
  GetOptions(\%snap_o,
             'dev=s',
             'action=s',
             'directory=s',
             'fstype=s',
             'sname=s',
             'size=s',
             'device-mount-point=s',
           );
  @ARGV = @ARGS_SAVE;
  $::PL->d('Snapshot action:', $snap_o{'action'});
  if($snap_o{'action'} eq 'get-vm-device-details') {
    print(STDOUT "device=/dev/null\n");
    print(STDOUT "snapshot-device=/dev/null\n");
    print(STDOUT "device-mount-point=null\n");
    print(STDOUT "filesystem-type=null\n");
    print(STDOUT "relative-copy-dir=0;0;0\n");
    print(STDOUT "snapshot-mount-point=/tmp\n");
  }
  return 0;
}

sub main {
  @ARGV = @_;
  %o = ( 'backupset-config' => $ENV{ZRM_CONF} );
  %c = ();

  GetOptions(\%o,
             'host=s',
             'user=s',
             'password=s',
             'port=s',
             'quiet',
             'backupset-config=s',
             'backup-dir=s',
             'type-of-dir=s',

             'mysqlhotcopy',
             'create-link',
             'source-host=s',
             'source-file=s',
             'destination-host=s',
             'destination-directory=s',

             'snapshot-parameters=s',
             'snapshot-command',
             'sname=s',
           );

  ## Load the config file given to us by ZRM.
  unless( $o{'backupset-config'} and
            %c = IniFile::read_config($o{'backupset-config'}) ) {
    die "Unable to open the config provided by ZRM or a test harness.\n";
  }
  ## ZRM uses INI-like configs that don't have groups. IniFile module
  ## treats global options as belonging to the empty-name group.
  %c = %{$c{''}};

  ## Setup our default values.
  $c{'xtrabackup-agent:port'} ||= $REMOTE_PORT;
  $c{'xtrabackup-client:logpath'} ||= '/var/log/mysql-zrm/xtrabackup-client.log';
  $c{'xtrabackup-client:email'}   ||= '';

  ## The default stream encoding is left *unset* so that this
  ## client can interoperate with older agents, at some point
  ## the default will be set to base64 or null encoding.
  # $c{'xtrabackup-client:stream-encoding'} = 'base64';

  ## Set the blocksize if it was not set in the configuration.
  ## The default block size is mostly a legacy holdover.
  $c{'xtrabackup-client:stream-block-size'} ||= DEFAULT_BLOCK_SIZE;

  if( not exists $c{'xtrabackup-client:tar-force-ownership'} ) {
    $c{'xtrabackup-client:tar-force-ownership'} = 1;
  }
  if( not exists $c{'xtrabackup-client:run-apply-log'} ) {
    $c{'xtrabackup-client:run-apply-log'} = 0;
  }

  ## Set the default network failure handling.
  if( not exists $c{'xtrabackup-client:on-network-failure'} ) {
    $c{'xtrabackup-client:on-network-failure'} = 'abort';
  }
  if( not exists $c{'xtrabackup-client:network-failure-retry'} ) {
    $c{'xtrabackup-client:network-failure-retry'} = 3;
  }
  if( not exists $c{'xtrabackup-client:network-timeout'} ) {
    $c{'xtrabackup-client:network-timeout'} = 30;
  }

  if( $c{'xtrabackup-client:on-network-failure'} !~ /^(abort|retry)$/ ) {
    $::PL->e('Unknown network-failure handling mode:',
             $1, ', defaulting to abort.');
    $c{'xtrabackup-client:on-network-failure'} = 'abort';
  }
  if( $c{'xtrabackup-client:network-failure-retry'} !~ /^(\d+)$/ ) {
    $::PL->e('Unknown network-failure-retry value:',
             $1, 'defaulting to 3.');
    $c{'xtrabackup-client:network-failure-retry'} = 3;
  }
  if( $c{'xtrabackup-client:network-timeout'} !~ /^(\d+)$/ ) {
    $::PL->e('Unknown network-timeout value:',
             $1, 'defaulting to 30.');
    $c{'xtrabackup-client:network-timeout'} = 30;
  }

  if( not exists $c{'xtrabackup-client:innobackupex-path'} ) {
    $c{'xtrabackup-client:innobackupex-path'} = Which::which('innobackupex-1.5.1');
  }

  ## We default to unpacking the backup because that's what
  ## all previous versions of this code has done.
  if(not exists $c{'xtrabackup-client:unpack-backup'}) {
    $c{'xtrabackup-client:unpack-backup'} = 1;
  }

  ## This is a really easy typo.. Work around it with a warning.
  if(exists $c{'xtrabackup-client:unpack-backups'}) {
    $::PL->i("Found 'xtrabackup-client:unpack-backups'\n",
             "Did you mean 'xtrabackup-client:unpack-backup'? Setting this one.");
    $c{'xtrabackup-client:unpack-backup'} = $c{'xtrabackup-client:unpack-backups'};
  }

  $::PL->logpath($c{'xtrabackup-client:logpath'});
  $::PL->quiet($c{'verbose'});
  $::PL->start;

  $::PL->d('arguments:', Dumper(\%o));
  $::PL->d('configuration:', Dumper(\%c));

  if($^O eq "linux") {
    $TAR_WRITE_OPTIONS = "--same-owner -cphsC";
    $TAR_READ_OPTIONS = "--same-owner -xphsC";
  }
  elsif($^O eq "freebsd") {
    $TAR_WRITE_OPTIONS = " -cph -f - -C";
    $TAR_READ_OPTIONS = " -xp -f - -C";
  }
  else {
    printAndDie("Unable to determine which tar options to use!");
  }

  if($c{'backup-level'} == 0 and exists $c{'compress'}
      and $c{'xtrabackup-client:unpack-backup'}==0) {
    $::PL->e("options: compress=1 and xtrabackup-client:unpack-backup=0\n",
             "This will cause ZRM to delete the backup data when compressing.\n",
             "Aborting backup."
            );
    my_exit(1);
  }

  if( $c{"xtrabackup-client:tar-force-ownership"} == 0
        or $c{"xtrabackup-client:tar-force-ownership"} =~ /[Nn][oO]?/ ) {
    $c{"xtrabackup-client:tar-force-ownership"} = 0;
    if( $^O eq "linux" ) {
      $TAR_WRITE_OPTIONS = "--no-same-owner --no-same-permissions -chsC";
      $TAR_READ_OPTIONS = "--no-same-owner --no-same-permissions -xhsC";
    }
    elsif( $^O eq "freebsd" ) {
      $TAR_WRITE_OPTIONS = " -ch -f - -C";
      $TAR_READ_OPTIONS  = " -x -f - -C";
    }
  }

  if($o{'source-host'} and $o{'source-file'}
      and $o{'destination-host'} and $o{'destination-directory'}) {
    my $agent_reply = {};
    my %more_headers = ();

    if(exists $c{'xtrabackup-client:stream-encoding'}) {
      if($c{'xtrabackup-client:stream-encoding'} eq 'base64') {
        $more_headers{'agent-stream-encoding'} = 'application/x-base64-stream';
      }
      elsif($c{'xtrabackup-client:stream-encoding'} eq 'null') {
        $more_headers{'agent-stream-encoding'} = 'application/octet-stream';
      }
      elsif($c{'xtrabackup-client:stream-encoding'} eq 'uuencode') {
        $more_headers{'agent-stream-encoding'} = 'application/x-uuencode-stream';
      }
      else {
        $::PL->e("Unknown encoding requested:",
                 $c{'xtrabackup-client:stream-encoding'}
                );
        my_exit(1);
      }
    }
    $more_headers{'agent-stream-block-size'} = $c{'xtrabackup-client:stream-block-size'};

    ## ZRM Calls this script multiple times in the case of incremental backups
    ## The SID (session id, roughly) allows us and the agent to identify when
    ## have been called before and to relate otherwise unrelated requests.
    ## We use the destination directory as the SID because it's virtually
    ## guaranteed to be unique. The only way it couldn't was if two backups
    ## for the same backupset were done in the same second. Highly unlikely.
    my $sid = $o{'destination-directory'};
    my $prev_backup = undef;
    my $next_binlog;

    if($o{'destination-directory'}) {
      $::PL->d('Attempting to open previous backup index to fetch binary log information.');
      ## This ridiculous dance is done because ZRM doesn't write out any amount
      ## of sensible to the index until after it's done with the backup, nor
      ## does it allow us to write to the index since it overwrites the index wholesale.
      ## Finally, we can't put anything in the backup directory that we want later,
      ## it'll be treated as backup data and deleted (after compression).
      eval {
        my $fh;
        my $last_dir;
        open($fh, "</etc/mysql-zrm/".
            ZRMBackup->new($o{'destination-directory'})->backup_set().
            "/last_backup"
            );
        chomp($last_dir = <$fh>);
        close($fh);
        $prev_backup = ZRMBackup->new($last_dir);
      };
      if($@) {
        $::PL->i("(Probably harmless): Unable to open last backup: $@");
      }
    }

    if($prev_backup and $prev_backup->next_binlog()) {
      $next_binlog = $prev_backup->next_binlog();
      $::PL->d('Found previous incremental backup, next binlog:', $next_binlog);
    }

    agentRequest($o{'source-host'}, $c{'xtrabackup-agent:port'},
      'copy from', $sid, 'file' => $o{'source-file'},
      'binlog' => $next_binlog, %more_headers);

    $agent_reply = agentRead();

    $::PL->d('Received reply from agent:', Dumper($agent_reply));

    if($c{'backup-level'} == 0) {
      if($agent_reply->{status} eq 'SENDING') {
        $::PL->m('Writing full backup to', $o{'destination-directory'});
        readInnoBackupStream();
      }
    }
    elsif($c{'backup-level'} == 1) {
      if($c{'replication'} == 0) {
        $::PL->i('With replication=0, you cannot make a new secondary master.');
      }

      ## Normally ZRM calls this script once for every binlog it wishes
      ## to have copied, however, this is needlessly inefficient.
      ## The first time we're called with a binlog parameter, we copy
      ## ALL the binlogs and then the server ignores further requests for binlogs
      ## with the same SID.
      ## When it ignores a request it sends 'status=OK', as the slave information.
      if($agent_reply->{status} eq 'SENDING') {
        delete $agent_reply->{status};
        my $fh;
        open($fh, ">$o{'destination-directory'}/master.info");
        print($fh join("\n",
              map {
                "$_=". (defined $$agent_reply{$_} ? $$agent_reply{$_} : 'NULL')
              } sort keys %$agent_reply));
        close($fh);
        readTarStream();
      }
    }
  }
  elsif($o{'snapshot-command'}) {
    $::PL->d('Trapped snapshot command, doing dummy actions.');
    dummySnapshotDeviceInfo();
  }

  close( SOCK );
  select( undef, undef, undef, 0.250 );

  my_exit(0);

}

if(!caller) { exit(main(@ARGV)); }

1;
