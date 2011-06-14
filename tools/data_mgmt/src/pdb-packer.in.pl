#!/usr/bin/env perl
# Copyright (c) 2009-2011, PalominoDB, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#
#   * Neither the name of PalominoDB, Inc. nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
use strict;
use warnings FATAL => 'all';

# ###########################################################################
# ProcessLog package 876f85f39dfeb6100fbb852f82cbf61c1e4d739a
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

sub DESTROY {
  my ($self) = @_;
  if(ref($$self{'LOG'}) and ref($$self{'LOG'}) eq 'GLOB') {
    $$self{'LOG'}->flush();
  }
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
  return $self->{log_path} if(not $logpath);
  $self->{log_path} = $logpath;
  if($logpath =~ /^syslog:(\w+)/) {
    require Sys::Syslog;
    Sys::Syslog::openlog($script_name, "", $1);
    $self->{logsub} = sub {
      my $self = shift;
      $_[3] = '';
      my $lvl = 'LOG_DEBUG';
      $lvl = 'LOG_INFO' if($_[0] eq "msg");
      $lvl = 'LOG_NOTICE' if($_[0] eq "ifo");
      $lvl = 'LOG_ERR'  if($_[0] eq "err");
      Sys::Syslog::syslog($lvl, _p(@_));
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
    binmode($self->{LOG});
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
  my ($self, $level, $top) = @_;
  $level = $self->{stack_depth} ||= 10 unless($level);
  $top   = (defined $top ? $top : 2);
  my $out = "";
  my $i=0;
  my ($package, $file, $line, $sub) = caller($i+$top); # +2 hides ProcessLog from the stack trace.
  $i++;
  if($package) {
    $out .= "Stack trace:\n";
  }
  else {
    $out .= "No stack data available.\n";
  }
  while($package and $i < $level) {
    $out .= " "x$i . "$package  $file:$line  $sub\n";
    ($package, $file, $line, $sub) = caller($i+$top);
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
# MysqlInstance package c22894a246cf547e45f87e9205da293715ca34e1
# ###########################################################################
package MysqlInstance::Methods;
use strict;
use warnings FATAL => 'all';
use Carp;

sub new {
  my ($class, $start, $stop, $restart, $status, $config) = @_;
  my $self = {};
  $self->{start} = $start;
  $self->{stop} = $stop;
  $self->{restart} = $restart;
  $self->{status} = $status;
  $self->{config} = $config;
  return bless $self, $class;
}

sub detect {
  my ($class) = @_;

  if($^O eq 'linux') {
    return $class->new(_identify_linux());
  }
  elsif($^O eq 'freebsd') {
    return $class->new(_identify_freebsd());
  }
  return $class->new();
}

sub _identify_linux {
  if($^O eq 'linux') {
    if(-f '/etc/debian_version') {
      if( ! -f '/etc/init.d/mysql' or ! -f '/etc/mysql/my.cnf' ) {
          return (undef, undef, undef, undef, undef);
      }
      return (
        '/etc/init.d/mysql start &>/dev/null',
        '/etc/init.d/mysql stop &>/dev/null',
        '/etc/init.d/mysql restart &>/dev/null',
        '/etc/init.d/mysql status &>/dev/null',
        '/etc/mysql/my.cnf'
      );
    }
    elsif( -f '/etc/redhat-release' ) {
      if( ! -f '/etc/init.d/mysql' or ! -f '/etc/my.cnf' ) {
          return (undef, undef, undef, undef, undef);
      }
      return (
        '/etc/init.d/mysql start 2>&1 | grep -q OK',
        '/etc/init.d/mysql stop  2>&1 | grep -q OK',
        '/etc/init.d/mysql restart 2>&1 | grep -q OK',
        '/etc/init.d/mysql status &>/dev/null',
        '/etc/my.cnf'
      );
    }
  }
  return (undef, undef, undef, undef, undef);
}

sub _identify_freebsd {
  if($^O eq 'freebsd' and -f '/usr/local/etc/rc.d/mysql-server') {
      return (
        '/usr/local/etc/rc.d/mysql-server start &>/dev/null',
        '/usr/local/etc/rc.d/mysql-server stop  &>/dev/null',
        '/usr/local/etc/rc.d/mysql-server restart &>/dev/null',
        '/usr/local/etc/rc.d/mysql-server status &>/dev/null',
        '/etc/my.cnf'
      );
  }
  return (undef, undef, undef, undef, undef);
}

1;


package MysqlInstance;
use strict;
use warnings FATAL => 'all';

use Carp;

use DBI;

sub new {
  my ($class, $mycnf, $methods)  = @_;
  my $self = {};
  $self->{mycnf}    = $mycnf;
  $self->{methods}  = $methods || MysqlInstance::Methods->detect();
  bless $self, $class;
  return $self;
}

sub stop {
  my ($self) = @_;
  system($self->{methods}->{stop}) >> 8;
}

sub start {
  my ($self) = @_;
  system($self->{methods}->{start}) >> 8;
}

sub restart {
  my ($self) = @_;
  system($self->{methods}->{restart}) >> 8;
}

sub status {
  my ($self) = @_;
  system($self->{methods}->{status}) >> 8;
}

sub config {
  my ($self) = @_;
  my $cfg = undef;
  eval {
    $cfg = {IniFile::read_config($self->{mycnf} || $self->{methods}->{config})};
  };
  if($@ or not defined $cfg) {
    die("Unable to open ".
          ($self->{mycnf} || $self->{methods}->{config})
            .": ". ($@ ? $@ : 'unknown reason'));
  }
  return $cfg;
}

sub methods {
  my ($self, $new_methods) = @_;
  my $old_methods = $self->{methods};
  $self->{methods} = $new_methods || $old_methods;
  return $old_methods;
}

sub remote {

  my ($class, $dsn, $action, @args) = @_;
  my $ro = RObj->new($dsn);
  $ro->add_package('IniFile');
  $ro->add_package('MysqlInstance::Methods');
  $ro->add_package('MysqlInstance');
  $ro->add_main(sub {
      my $act = shift;
      my $mi = MysqlInstance->new(@_);
      if($act eq 'stop') {
        return $mi->stop();
      }
      elsif($act eq 'start') {
        return $mi->start();
      }
      elsif($act eq 'restart') {
        return $mi->restart();
      }
      elsif($act eq 'status') {
        return $mi->status();
      }
      elsif($act eq 'config') {
        return $mi->config();
      }
    });
  return [$ro->do($action, @args)];
}

1;


# ###########################################################################
# End MysqlInstance package
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
# IniFile package d70faf2773ed7da1be74ef0675cf06f3f0c57122
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

      if($k =~ /^(.*?)\s*\[\s*(\d+)?\s*\]/) {
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
# DSN package 13f9a3c9df3506bad80034eedeb6ba834aa1444d
# ###########################################################################
package DSN;
use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Storable;

sub _create {
  my ($class, $keys) = @_;
  my $self = {};
  $self = _merge($self, $keys);
  return bless $self, $class;
}

sub STORABLE_freeze {
  my ($self, $cloning) = @_;
  return if $cloning;
  my $f = {};
  _merge($f, $self);
  return (
    Storable::nfreeze($f)
  );
}

sub STORABLE_thaw {
  my ($self, $cloning, $serialized) = @_;
  return if $cloning;
  my $f = Storable::thaw($serialized);
  return _merge($self, $f);
}

sub STORABLE_attach {
  my ($class, $cloning, $serialized) = @_;
  return if $cloning;
  my $f = Storable::thaw($serialized);
  return $class->_create($f);
}

sub DESTROY {}

sub get {
  my ($self, $k) = @_;
  return $self->{$k}->{'value'};
}

sub has {
  my ($self, $k) = @_;
  return exists $self->{$k}->{'value'};
}

sub str {
  my ($self) = @_;
  my $str = "";
  for(sort keys %$self) {
    $str .= "$_=". $self->get($_) ."," if($self->has($_));
  }
  chop($str);
  return $str;
}

sub get_dbi_str {
  my ($self, $extra_opts) = @_;
  $extra_opts ||= {};
  my %set_implied = ();
  my %dsn_conv = (
    'h' => 'host',
    'P' => 'port',
    'F' => 'mysql_read_default_file',
    'G' => 'mysql_read_default_group',
    'S' => 'mysql_socket',
    'D' => 'database',
    'SSL_key' => 'mysql_ssl_client_key',
    'SSL_cert' => 'mysql_ssl_client_cert',
    'SSL_CA' => 'mysql_ssl_ca_file',
    'SSL_CA_path' => 'mysql_ssl_ca_path',
    'SSL_cipher' => 'mysql_ssl_cipher'
  );
  my %opt_implied = (
    'SSL_key' => 'mysql_ssl=1',
    'SSL_cert' => 'mysql_ssl=1',
    'SSL_CA' => 'mysql_ssl=1',
    'SSL_CA_path' => 'mysql_ssl=1',
    'SSL_cipher' => 'mysql_ssl=1'
  );

  my $dbh_str = 'DBI:mysql:';

  for(sort keys(%$self)) {
    if(exists($opt_implied{$_}) and $self->has($_) and !$set_implied{$opt_implied{$_}}) {
      $dbh_str .= $opt_implied{$_} . ';';
      $set_implied{$opt_implied{$_}} = 1;
    }
    $dbh_str .= $dsn_conv{$_} .'='. ($self->get($_) || '') .';'
    if(exists($dsn_conv{$_}) and $self->has($_));
  }
  if(%$extra_opts) {
    $dbh_str .= join(';',
      map { "$_=". $$extra_opts{$_} } sort keys(%$extra_opts));
  }
  return $dbh_str;
}

sub get_dbh {
  my ($self, $cached, $extra_opts, $extra_dbi_opts) = @_;
  my $dbh_str = $self->get_dbi_str($extra_dbi_opts);
  my $options = _merge({ 'AutoCommit' => 0, 'RaiseError' => 1,
        'PrintError' => 0, 'ShowErrorStatement' => 1 }, ($extra_opts || {}));
  my $dbh;

  if($cached) {
    $dbh = DBI->connect_cached($dbh_str, $self->get('u'), $self->get('p'),
      $options);
  }
  else {
    $dbh = DBI->connect($dbh_str, $self->get('u'), $self->get('p'),
      $options);
  }
  if($self->has('N')) {
    $dbh->do('SET NAMES '. $dbh->quote($self->get('N')));
  }
  if($self->has('vars')) {
    my $vars = join(', ', map {
        my ($k, $v) = split(/=/, $_, 2);
        $_ = $k . ' = ' . ($v =~ /^\d+$/ ? $v : $dbh->quote($v, 1));
        $_;
      } split(/;/, $self->get('vars')));
    $dbh->do('SET '. $vars);
  }
  return $dbh;
}

sub fill_in {
  my ($self, $from) = @_;
  $self = _merge($self, $from, 0);
  return $self;
}

sub _merge {
  my ($h1, $h2, $over, $p) = @_;
  foreach my $k (keys %$h2) {
    if(!ref($h2->{$k})) {
      if($over and exists $h1->{$k}) {
        $h1->{$k} = $h2->{$k};
      }
      elsif(!exists $h1->{$k}) {
        $h1->{$k} = $h2->{$k};
      }
    }
    elsif(ref($h2->{$k}) eq 'ARRAY') {
      $h1->{$k} = [];
      push @{$h1->{$k}}, $_ for(@{$h2->{$k}});
    }
    else {
      $h1->{$k} ||= {};
      _merge($h1->{$k}, $h2->{$k}, $over, $h1);
    }
  }
  $h1;
}

1;


package DSNParser;
use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Carp;

sub new {
  my ($class, $keys) = @_;
  croak('keys must be a hashref') unless(ref($keys));
  my $self = {};
  $self->{'keys'} = $keys;
  $self->{'allow_unknown'} = 0;
  return bless $self, $class;
}

sub add_key {
  my ($self, $key, $params) = @_;
  croak('params must be a hashref') unless(ref($params));
  if(exists $self->{'keys'}->{$key}) {
    croak("Key '$key' must not already exist");
  }
  $self->{'keys'}->{$key} = $params;
}

sub rem_key {
  my ($self, $key) = @_;
  unless(exists $self->{'keys'}->{$key}) {
    croak("Key '$key' must already exist");
  }
  delete $self->{'keys'}->{$key};
}

sub mand_key {
  my ($self, $key, $flag) = @_;
  unless(exists $self->{'keys'}->{$key}) {
    croak("Key '$key' must already exist");
  }
  $self->{'keys'}->{$key}->{'mandatory'} = $flag;
}

sub default {
  my ($class) = @_;
  my $default_keys = {
    'h' => {
      'desc' => 'Hostname',
      'default' => '',
      'mandatory' => 0
    },
    'u' => {
      'desc' => 'Username',
      'default' => '',
      'mandatory' => 0
    },
    'p' => {
      'desc' => 'Password',
      'default' => '',
      'mandatory' => 0
    },
    'P' => {
      'desc' => 'Port',
      'default' => 3306,
      'mandatory' => 0
    },
    'F' => {
      'desc' => 'Defaults File',
      'default' => '',
      'mandatory' => 0
    },
    'G' => {
      'desc' => 'Defaults File Group',
      'default' => 'client',
      'mandatory' => 0
    },
    'D' => {
      'desc' => 'Database name',
      'default' => '',
      'mandatory' => 0
    },
    't' => {
      'desc' => 'Table name',
      'default' => '',
      'mandatory' => 0
    },
    'S' => {
      'desc' => 'Socket path',
      'default' => '',
      'mandatory' => 0
    },
    'N' => {
      'desc' => 'Client character set',
      'default' => '',
      'mandatory' => 0
    },
    'vars' => {
      'desc' => 'Extra client variables',
      'default' => '',
      'mandatory' => 0
    },
    'sU' => {
      'desc' => 'SSH User',
      'default' => '',
      'mandatory' => 0
    },
    'sK' => {
      'desc' => 'SSH Key',
      'default' => '',
      'mandatory' => 0
    },
    'SSL_key' => {
      'desc' => 'SSL client key',
      'default' => '',
      'mandatory' => 0
    },
    'SSL_cert' => {
      'desc' => 'SSL client certificate',
      'default' => '',
      'mandatory' => 0
    },
    'SSL_CA' => {
      'desc' => 'SSL client CA file',
      'default' => '',
      'mandatory' => 0
    },
    'SSL_CA_path' => {
      'desc' => 'SSL client CA path',
      'default' => '',
      'mandatory' => 0
    },
    'SSL_cipher' => {
      'desc' => 'SSL cipher',
      'default' => '',
      'mandatory' => 0
    }
  };
  return $class->new($default_keys);
}

sub parse {
  my ($self, $str) = @_;
  use Data::Dumper;
  $Data::Dumper::Indent = 0;
  my $dsn = DSN->_create($self->{'keys'});
  foreach my $kv ( split(/,/, $str) ) {
    my ($key, $val) = split(/=/, $kv, 2);
    croak('Unknown key: '. $key .' in dsn')
    unless($self->{'allow_unknown'} or exists($self->{'keys'}->{$key}) );
    $dsn->{$key}->{'value'} = $val;
  }
  foreach my $k ( keys %$dsn ) {
    if($dsn->{$k}->{'mandatory'} and ! exists($dsn->{$k}->{'value'})) {
      croak('Missing key: '. $k .' ['. ($self->{'keys'}->{$k}->{'desc'}||'no description') .'] in dsn');
    }
  }
  return $dsn;
}


1;
# ###########################################################################
# End DSN package
# ###########################################################################

# ###########################################################################
# TableAge package b6b340d3dab50d36e0cd373caa4f7393616cab2c
# ###########################################################################
package TableAge;
use strict;
use warnings FATAL => 'all';
use Data::Dumper;
use DateTime::Format::Strptime;

sub new {
  my $class = shift;
  my ($dbh, $pattern) = @_;
  my $self = {};
  $self->{dbh} = $dbh;
  $self->{pattern} = $pattern;
  $self->{status_dft} = DateTime::Format::Strptime->new(
    pattern => '%F %T', time_zone => "local");
  $self->{name_dft} =  DateTime::Format::Strptime->new(
    pattern => $pattern, time_zone => "local");
  return bless $self, $class;
}

sub age_by_status {
  my ($self, $schema, $table) = @_;
  my $status = $self->{dbh}->selectrow_hashref(qq|SHOW TABLE STATUS FROM `$schema` LIKE '$table'|);
  return $self->{status_dft}->parse_datetime($status->{'Create_time'});
}

sub age_by_name {
  my ($self, $table, $pattern) = @_;
  if($pattern) {
    $self->{name_dft}->pattern($pattern);
  }
  return $self->{name_dft}->parse_datetime($table);
}

sub older_than {
  my ($self, $tbl_age, $when) = @_;
  if(DateTime->compare($tbl_age, $when) == -1) {
    return 1;
  }
  return 0;
}

sub newer_than {
  my ($self, $tbl_age, $when) = @_;
  if(DateTime->compare($tbl_age, $when) == 1) {
    return 1;
  }
  return 0;
}

1;
# ###########################################################################
# End TableAge package
# ###########################################################################

# ###########################################################################
# TablePacker package acb476fdc1ee2ddff9e6fbe0f96b17c9fcc31d23
# ###########################################################################
package TablePacker;
use strict;
use warnings FATAL => 'all';

use Carp;

use DBI;
use Storable;

sub new {
  my $class = shift;
  my ($dsn, $datadir, $dbh) = @_;
  croak("dsn must be a reference to a DSN") unless(ref($dsn));
  my $self = {};
  $self->{datadir} = $datadir;
  $self->{dsn} = $dsn;
  if($dbh) {
    $self->{dbh} = $dbh;
  }
  else {
    $self->{own_dbh} = 1;
    $self->{dbh} = $dsn->get_dbh();
  }
  $self->{schema} = $dsn->get('D');
  $self->{table}  = $dsn->get('t');
  return bless $self, $class;
}

sub STORABLE_freeze {
  my ($self, $cloning) = @_;
  return if $cloning;
  return (
    Storable::nfreeze({
        myisamchk => $self->{myisamchk},
        myisampack => $self->{myisampack},
        datadir => $self->{datadir},
        dsn     => $self->{dsn},
        schema  => $self->{schema},
        table   => $self->{table},
        errstr  => $self->{errstr},
        errval  => $self->{errval}
      })
  );
}

sub STORABLE_thaw {
  my ($self, $cloning, $serialized) = @_;
  return if $cloning;
  my $frst = Storable::thaw($serialized);
  $self->{datadir} = $frst->{datadir};
  $self->{dsn} = $frst->{dsn};
  $self->{schema} = $frst->{schema};
  $self->{table} = $frst->{table};
  $self->{myisamchk} = $frst->{myisamchk};
  $self->{myisampack} = $frst->{myisampack};
  $self->{errstr} = $frst->{errstr};
  $self->{errval} = $frst->{errval};
  return $self;
}

sub STORABLE_attach {
  my ($class, $cloning, $serialized) = @_;
  return if $cloning;
  my $frst = Storable::thaw($serialized);
  my $self;
  eval {
    $self = $class->new($frst->{dsn}, $frst->{datadir}, undef);
  };
  if($@ and $@ =~ /DBI connect.*failed: Access denied/i) {
    $self = $class->new($frst->{dsn}, $frst->{datadir}, 'FakeDBH');
  }
  elsif($@) {
    croak($@);
  }
  $self->{myisamchk} = $frst->{myisamchk};
  $self->{myisampack} = $frst->{myisampack};
  $self->{errstr} = $frst->{errstr};
  $self->{errval} = $frst->{errval};
  return $self;
}

sub DESTROY {
  my ($self) = @_;
  if($self->{own_dbh}) {
    $self->{dbh}->disconnect();
  }
}

sub _reconnect {
  my ($self) = @_;
  eval {
    die('Default ping') if($self->{dbh}->ping == 0E0);
  };
  if($@ =~ /^Default ping/) {}
  elsif($@) {
    eval {
      $self->{own_dbh} = 1;
      $self->{dbh} = $self->{dsn}->get_dbh();
    };
    return 1;
  }
  return 0E0;
}

sub myisampack_path {
  my ($self, $path) = @_;
  my $old = $self->{myisampack};
  $self->{myisampack} = $path if( defined $path );
  $old;
}

sub myisamchk_path {
  my ($self, $path) = @_;
  my $old = $self->{myisamchk};
  $self->{myisamchk} = $path if( defined $path );
  $old
}

sub mk_myisam {
  my ($self, $note, $no_replicate) = @_;
  $no_replicate = 1 if(not defined $no_replicate);
  if($note) {
    $note = "/* $note */ ";
  }
  else {
    $note = '';
  }
  $self->_reconnect();
  my ($schema, $table) = ($self->{schema}, $self->{table});
  my $eng = $self->engine();
  my $typ = $self->format();
  my ($log_bin) = $self->{dbh}->selectrow_array('SELECT @@sql_log_bin');
  if($eng ne "myisam" and $typ ne 'compressed') {
    $self->{dbh}->do("SET sql_log_bin=0") if($no_replicate);
    $self->{dbh}->do($note ."ALTER TABLE `$schema`.`$table` ENGINE=MyISAM") or croak("Could not make table myisam");
    $self->{dbh}->do("SET sql_log_bin=$log_bin") if($no_replicate);
    return 1;
  }
  return 1;
}

sub check {
  my ($self) = @_;
  my ($datadir, $schema, $table) =
  ($self->{datadir}, $self->{schema}, $self->{table});
  my $myisamchk = ($self->{myisamchk} ||= Which::which('myisamchk'));
  my ($out, $res);

  $out = qx|$myisamchk -rq "${datadir}/${schema}/${table}" 2>&1|;
  $res = ($? >> 8);

  if($res) {
    $self->{errstr} = $out;
    $self->{errval} = $res;
    croak("Error checking table `$schema`.`$table`");
  }

  return 0;
}

sub flush {
  my ($self) = @_;
  my ($schema, $table) = ($self->{schema}, $self->{table});
  $self->_reconnect();
  $self->{dbh}->do("FLUSH TABLES `$schema`.`$table`");
}

sub pack {
  my ($self, $force) = @_;
  my ($datadir, $schema, $table) =
  ($self->{datadir}, $self->{schema}, $self->{table});
  my $myisampack = ($self->{myisampack} ||= Which::which('myisampack'));
  my ($out, $res);

  $force = $force ? '--force' : '';

  $out = qx|$myisampack $force "${datadir}/${schema}/${table}" 2>&1|;
  $res = ($? >> 8);

  if($res) {
    $self->{errstr} = $out;
    $self->{errval} = $res;
    croak("Error packing table `$schema`.`$table`");
  }

  return 0;
}

sub unpack {
  my ($self) = @_;
  my ($datadir, $schema, $table) =
  ($self->{datadir}, $self->{schema}, $self->{table});
  my $myisamchk = ($self->{myisamchk} ||= Which::which('myisamchk'));
  my ($out, $res);

  $out = qx|$myisamchk --unpack "${datadir}/${schema}/${table}" 2>&1|;
  $res = ($? >> 8);

  if($res) {
    $self->{errstr} = $out;
    $self->{errval} = $res;
    croak("Error checking table `$schema`.`$table`");
  }

  return 0;
}

sub engine {
  my ($self) = @_;
  my ($schema, $table) = ($self->{schema}, $self->{table});
  my $eng;
  $self->_reconnect();
  eval {
    $eng = $self->{dbh}->selectrow_hashref("SHOW TABLE STATUS FROM `$schema` LIKE '$table'")->{'Engine'};
  };
  if($@ =~ /undefined value as a HASH/i) { croak("Table `$schema`.`$table` does not exist") }
  elsif($@) { croak($@); }
  return lc($eng);
}

sub format {
  my ($self) = @_;
  my ($schema, $table) = ($self->{schema}, $self->{table});
  my $typ;
  $self->_reconnect();
  eval {
    $typ = $self->{dbh}->selectrow_hashref("SHOW TABLE STATUS FROM `$schema` LIKE '$table'")->{'Row_format'};
  };
  if($@ =~ /undefined value as a HASH/i) { croak("Table `$schema`.`$table` does not exist") }
  elsif($@) { croak($@); }
  return lc($typ);
}

1;
# ###########################################################################
# End TablePacker package
# ###########################################################################

# ###########################################################################
# TableRotater package ae68c90f28ffc16e059a8aea3248d3e1e3edff97
# ###########################################################################
package TableRotater;
use DBI;

use DateTime;
use Carp;

sub new {
  my $class = shift;
  my ($dsn, $format, $dbh) = @_;
  $format ||= "%Y%m%d";
  my $self = {};
  if($dbh) {
    $self->{dbh} = $dbh;
  }
  else {
    $self->{dbh} = $dsn->get_dbh();
    $self->{own_dbh} = 1;
  }
  $self->{format} = $format;

  return bless $self, $class;
}

sub DESTROY {
  my ($self) = @_;
  if($self->{own_dbh}) {
    $self->{dbh}->disconnect();
  }
}

sub date_rotate_name {
  my ($self, $table, $dt) = @_;
  $dt ||= DateTime->now(time_zone => 'local');
  my $rot_table = $dt->strftime("${table}$self->{format}");
}

sub rand_str {
  my ($self) = @_;

  my @chars=('a'..'z','A'..'Z','0'..'9','_');
  my $random_string;
  foreach (1..16) {
    $random_string.=$chars[rand @chars];
  }
  return $random_string;
}

sub table_for_date {
  my ($self, $schema, $table, $dt) = @_;
  my $rot_table = $self->date_rotate_name($table, $dt);
  $self->{dbh}->selectrow_hashref(
    qq|SHOW TABLE STATUS FROM `$schema` LIKE '$rot_table'|
  );
}

sub date_rotate {
  my ($self, $schema, $table, $dt) = @_;

  my $rot_table = $self->date_rotate_name($table, $dt);
  my $tmp_table = "${table}_". $self->rand_str();

  local $SIG{INT};
  local $SIG{TERM};
  local $SIG{HUP};

  eval {
    $self->{dbh}->do(
      "CREATE TABLE `$schema`.`$tmp_table` LIKE `$schema`.`$table`"
    ) 
  };
  if($@) {
    $self->{errstr} = $@;
    croak("Unable to create new table $tmp_table");
  }

  eval {
    $self->{dbh}->do(
      "RENAME TABLE 
        `$schema`.`$table` TO `$schema`.`$rot_table`,
        `$schema`.`$tmp_table` TO `$schema`.`$table`"
      );
  };
  if($@) {
    $self->{errstr} = $@;
    croak("Failed to rename table to $rot_table, $tmp_table");
  }
  return $rot_table;
}

1;
# ###########################################################################
# End TableRotater package
# ###########################################################################

# ###########################################################################
# RObj package d56edd1fbf95ad1e87eed4e6b588ad94af5bfae3
# ###########################################################################
package RObj::Base;
use strict;
use warnings FATAL => 'all';
use 5.0008;
use English qw(-no_match_vars);
use Storable qw(thaw nfreeze);
use MIME::Base64;
use Carp;

use Data::Dumper;

use Exporter;
use vars qw(@ISA $VERSION @EXPORT);

@ISA = qw(Exporter);
@EXPORT = qw(COMPILE_FAILURE TRANSPORT_FAILURE OK);

$VERSION = 0.01;

use constant NATIVE_DEATH      => -3;
use constant COMPILE_FAILURE   => -2;
use constant TRANSPORT_FAILURE => -1;
use constant OK => 0;

use constant ROBJ_NET_DEBUG => ($ENV{'ROBJ_NET_DEBUG'} || 0);

sub new {
  my $class = shift;
  my $self = {};
  bless $self, $class;
  $self->{Msg_Buffer} = "";
  $self->{Sys_Error} = 0;
  return $self;
}

sub read_message {
  my ($self, $fh) = @_;
  my ($buf, @res) = ("", ());
  $self->{Sys_Error} = 0;
  if( !sysread( $fh, $buf, 10240) ) {
    $self->{Sys_Error} = $!;
    return undef;
  }
  ROBJ_NET_DEBUG && print STDERR "recv(". length($buf) ."b): $buf\n";
  $self->{Msg_Buffer} .= $buf;
  if($self->{Msg_Buffer} =~ /^ok$/m) {
    ROBJ_NET_DEBUG >=2 && print STDERR "recv: Found message delimiter\n";
    my @lines = split /\n/, $self->{Msg_Buffer};
    my $b64 = "";
    for (@lines) {
      ROBJ_NET_DEBUG >=2 && print STDERR "recv: parsing: $_\n";
      if(/^ok$/) {
        ROBJ_NET_DEBUG >=2 && print STDERR "recv: found complete object\n";
        eval {
          push @res, @{thaw(decode_base64($b64))};
        };
        if($EVAL_ERROR) {
          push @res, ['INVALID MESSAGE', $EVAL_ERROR, "${b64}\n"];
        }
        $b64 = "";
      }
      elsif(/^[A-Za-z0-9+\/=]+$/) {
        $b64 .= "$_\n";
      }
      else {
        ROBJ_NET_DEBUG >=2 && print STDERR "recv: ignoring garbage\n";
      }
    }
    $self->{Msg_Buffer} = $b64;
  }
  ROBJ_NET_DEBUG && print STDERR "recv obj: ". Dumper(\@res);
  return @res;
}

sub write_message {
  my ($self, $fh, @objs) = @_;
  my $buf;
  eval {
    $buf = encode_base64(nfreeze(\@objs));
  };
  if($EVAL_ERROR) {
    croak $EVAL_ERROR;
  }
  $self->{Sys_Error} = 0;
  ROBJ_NET_DEBUG && print STDERR "send(". length($buf) ."b): ${buf}ok\n";
  return syswrite($fh, $buf ."ok\n");
}

sub sys_error {
  my ($self) = @_;
  return  $self->{Sys_Error};
}

1;
package RObj;
use strict;
use warnings FATAL => 'all';
use 5.008;

use Storable qw(nfreeze thaw);
use MIME::Base64;
use IPC::Open3;
use IO::Select;
use IO::Handle;
use POSIX;
use Exporter;
use B::Deparse;
use Carp;

use Data::Dumper;

$Data::Dumper::Indent = 0;
$Data::Dumper::Sortkeys = 1;

RObj::Base->import;

use vars qw(@ISA @EXPORT $VERSION);
$VERSION = 0.01;
@ISA = qw(Exporter RObj::Base);

@EXPORT = qw(R_die R_exit R_read R_write COMPILE_FAILURE TRANSPORT_FAILURE OK);

{
  no warnings 'once';
  $Storable::Deparse = 1;
}


sub new {
  my ($class, $host, $user, $ssh_key, $pw_auth) = @_;
  my $s = RObj::Base->new;
  bless $s, $class;

  if(ref($host) and ref($host) eq 'DSN') {
    $s->{host} = $host->get('h');
    $s->{user} = $host->get('sU');
    $s->{ssh_key} = $host->get('sK');
  }
  else {
    $s->{host} = $host;
    $s->{user} = $user;
    $s->{ssh_key} = $ssh_key;
  }
  $s->{code} = ();
  $s->{recvq} = ();
  $s->{password_auth} = $pw_auth;
  return $s;
}

sub copy {
  my ($self) = @_;
  my $s = RObj::Base->new;
  bless $s, ref($self);
  $s->{host} = $self->{host};
  $s->{user} = $self->{user};
  $s->{ssh_key} = $self->{ssh_key};
  $s->{password_auth} = $self->{password_auth};
  $s->{code} = ();
  $s->{recvq} = ();
  return $s;
}

sub _pong_end {
  return 'ok';
}

sub check {
  my ($self, $cksub) = @_;
  my @r;
  my $ro = $self->copy;
  $ro->add_main($cksub || \&_pong_end);
  eval {
    @r = $ro->do();
    unless($r[1] eq 'ok') {
      croak($r[1]);
    }
  };
  chomp($@);
  croak('failed check: '. $@) if($@);
  return @r;
}

sub add_main {
  my ($self, $coderef) = @_;
  croak("Not a coderef") unless ref($coderef) eq 'CODE';
  push @{$self->{code}}, ['R_main', $coderef];
}

sub add_sub {
  my ($self, $name, $coderef) = @_;
  croak("Not a coderef") unless ref($coderef) eq 'CODE';
  push @{$self->{code}}, [$name, $coderef];
}

sub add_use {
  my ($self, $to, $pkg) = @_;
  unshift @{$self->{code}}, ["_use_$to", eval qq|sub { eval "package $to; use $pkg; 1;"; }| ];
}

sub add_package {
  my ($self, $pkg_name) = @_;
  no strict 'refs';
  die('Package '. $pkg_name .' empty - did you load it?') if(!%{"${pkg_name}::"});
  foreach my $s (sort keys %{"${pkg_name}::"}) {
    next if $s eq 'BEGIN';
    $self->add_sub($pkg_name . '::' . $s,\&{${"${pkg_name}::"}{$s}} );
  }
  return 0;
}

sub read {
  my ($self) = @_;
  my @recv;
  if($self->{recvq} and scalar @{$self->{recvq}}) {
    return shift @{$self->{recvq}};
  }
  1 while( !(@recv = $self->read_message($self->{ssh_ofh})) and $self->sys_error() == 0 );
  push @{$self->{recvq}}, @recv;
  return shift @{$self->{recvq}};
}

sub read_err {
  my ($self) = @_;
  my $buf;
  sysread($self->{ssh_efh}, $buf, 10240);
  return $buf;
}

sub write {
  my ($self, @objs) = @_;
  my $r = $self->write_message($self->{ssh_ifh}, @objs);
  $self->{ssh_ifh}->flush();
  return $r;
}

sub do {
  my ($self, @rparams) = @_;
  $self->start(@rparams);
  return $self->wait();
}

sub wait {
  my ($self) = @_;
  waitpid($self->{ssh_pid}, 0);
  my @r;
  push @r, $self->read();
  while (scalar @{$self->{recvq}}) {
    push @r, $self->read();
  }
  return @r;
}

sub debug {
  my ($self, $to) = @_;
  $self->{debug} = $to;
}

sub password_auth {
  my ($self, $allow) = @_;
  my $old_set = $self->{password_auth};
  if(defined $allow) {
    $self->{password_auth} = $allow;
  }
  return $old_set;
}

sub start {
  my ($self, @rparams) = @_;
  if(!@rparams) {
    @rparams = (undef);
  }
  my $code = $self->_wrap;
  my ($ssh_out, $ssh_err, $ssh_in, $exitv, $out, $err);
  $self->{ssh_pid} = open3($ssh_in, $ssh_out, $ssh_err,
    'ssh', $self->{ssh_key} ? ('-i', $self->{ssh_key}) : (),
    '-l', $self->{user}, $self->{host},
    '-o', $self->{password_auth} ? ('BatchMode=yes') : ('BatchMode=no'),
    $self->{debug} ?
      qq(PERLDB_OPTS="RemotePort=$self->{debug}" perl -d)
      : 'perl');

  syswrite($ssh_in, "$code\n\n");
  $ssh_in->flush();

  $self->{ssh_ifh} = $ssh_in;
  $self->{ssh_efh} = $ssh_err;
  $self->{ssh_ofh} = $ssh_out;
  my @r = ($self->read(), @{$self->{recvq}});
  $self->{recvq} = ();
  if(not $r[0] or $r[0] ne 'READY') {
    croak "Remote end did not come up properly. Expected: 'READY'; Got: ". (!$r[0] ? 'undef': join(' ',@r));
  }
  else {
    ProcessLog::_PdbDEBUG >= ProcessLog::Level2
    && $::PL->d("Sending parameters to remote:\n", Dumper(\@rparams));
    unless($self->write(@rparams)) {
      croak "Sending initial parameters to RObj failed.";
    }
    eval {
      local $SIG{ALRM} = sub { alarm 0; die 'alarm'; };
      alarm 5;
      @r = $self->read();
      alarm 0;
    };
    if($r[0] ne 'ACK' or $@ eq 'alarm') {
      croak 'Remote end did not pick up our args';
    }
  }
}


sub _wrap {
  my ($self) = @_;
  my $code = ();
  my $dp = B::Deparse->new('-P');
  $dp->ambient_pragmas(strict => 'all', warnings => [FATAL => 'all']);
  foreach my $c(@{$self->{code}}) {
    my $ctxt = $dp->coderef2text($c->[1]);
    if($c->[0] !~ /::/) {
      $ctxt =~ s/package.*;//m;
    }
    elsif($ctxt eq ';') {
      $ctxt = '{ }';
    }
    push @$code, [$c->[0], $ctxt];
  }
  ProcessLog::_PdbDEBUG >= ProcessLog::Level3
  && $::PL->d("Decompiled code to be serialized:\n", Dumper($code));
  $code = encode_base64(nfreeze($code));
  my $cnt =<<'EOF';
package RObj::Base;
use strict;
use warnings FATAL => 'all';
use 5.0008;
use English qw(-no_match_vars);
use Storable qw(thaw nfreeze);
use MIME::Base64;
use Carp;

use Data::Dumper;

use Exporter;
use vars qw(@ISA $VERSION @EXPORT);

@ISA = qw(Exporter);
@EXPORT = qw(COMPILE_FAILURE TRANSPORT_FAILURE OK);

$VERSION = 0.01;

use constant NATIVE_DEATH      => -3;
use constant COMPILE_FAILURE   => -2;
use constant TRANSPORT_FAILURE => -1;
use constant OK => 0;

use constant ROBJ_NET_DEBUG => ($ENV{'ROBJ_NET_DEBUG'} || 0);

sub new {
  my $class = shift;
  my $self = {};
  bless $self, $class;
  $self->{Msg_Buffer} = "";
  $self->{Sys_Error} = 0;
  return $self;
}

sub read_message {
  my ($self, $fh) = @_;
  my ($buf, @res) = ("", ());
  $self->{Sys_Error} = 0;
  if( !sysread( $fh, $buf, 10240) ) {
    $self->{Sys_Error} = $!;
    return undef;
  }
  ROBJ_NET_DEBUG && print STDERR "recv(". length($buf) ."b): $buf\n";
  $self->{Msg_Buffer} .= $buf;
  if($self->{Msg_Buffer} =~ /^ok$/m) {
    ROBJ_NET_DEBUG >=2 && print STDERR "recv: Found message delimiter\n";
    my @lines = split /\n/, $self->{Msg_Buffer};
    my $b64 = "";
    for (@lines) {
      ROBJ_NET_DEBUG >=2 && print STDERR "recv: parsing: $_\n";
      if(/^ok$/) {
        ROBJ_NET_DEBUG >=2 && print STDERR "recv: found complete object\n";
        eval {
          push @res, @{thaw(decode_base64($b64))};
        };
        if($EVAL_ERROR) {
          push @res, ['INVALID MESSAGE', $EVAL_ERROR, "${b64}\n"];
        }
        $b64 = "";
      }
      elsif(/^[A-Za-z0-9+\/=]+$/) {
        $b64 .= "$_\n";
      }
      else {
        ROBJ_NET_DEBUG >=2 && print STDERR "recv: ignoring garbage\n";
      }
    }
    $self->{Msg_Buffer} = $b64;
  }
  ROBJ_NET_DEBUG && print STDERR "recv obj: ". Dumper(\@res);
  return @res;
}

sub write_message {
  my ($self, $fh, @objs) = @_;
  my $buf;
  eval {
    $buf = encode_base64(nfreeze(\@objs));
  };
  if($EVAL_ERROR) {
    croak $EVAL_ERROR;
  }
  $self->{Sys_Error} = 0;
  ROBJ_NET_DEBUG && print STDERR "send(". length($buf) ."b): ${buf}ok\n";
  return syswrite($fh, $buf ."ok\n");
}

sub sys_error {
  my ($self) = @_;
  return  $self->{Sys_Error};
}

1;

package main;
use strict;
use warnings FATAL => 'all';
use 5.0008;
BEGIN {
  $SIG{__DIE__} = sub {
    die @_ if $^S;
    my $ro = RObj::Base->new;
    $ro->write_message(\*STDOUT, @_);
    exit(RObj::Base::COMPILE_FAILURE);
  };
}
use Storable qw(nfreeze thaw);
use MIME::Base64;
use IO::Handle;
use English qw(-no_match_vars);

RObj::Base->import;

use constant COMPILE_FAILURE => RObj::Base::COMPILE_FAILURE;
use constant TRANSPORT_FAILURE => RObj::Base::TRANSPORT_FAILURE;
use constant NATIVE_DEATH => RObj::Base::NATIVE_DEATH;
use constant OK => RObj::Base::OK;

my $ro = RObj::Base->new;

sub R_die {
  my ($die_code, $msg) = @_;
  my @caller_ifo = caller(0);
  R_print($msg . "at $caller_ifo[1] line $caller_ifo[2].");
  R_exit($die_code);
}

sub R_exit {
  my ($exit_code) = @_;
  R_print('EXIT', $exit_code);
  exit(OK);
}

sub R_print {
  $ro->write_message(\*STDOUT, @_);
}

sub R_read {
  my @recv;
  1 while( !(@recv = $ro->read_message(\*STDIN)) and $ro->sys_error() == 0 );
  return @recv;
}

use constant CODE => '__CODE__';
$0 = "Remote perl object from ". ($ENV{'SSH_CLIENT'} || 'localhost');

my $code = thaw(decode_base64(CODE));

{
  no strict 'refs';
  foreach my $cr (@{$code}) {
    my $name = $cr->[0];
    if($name =~ /^_use_/ ) {
      &{eval "sub $cr->[1]"}();
      if($@) {
        R_die(COMPILE_FAILURE, "Unable to use ($name). eval: $@");
      }
      next;
    }
    if($name =~ /::BEGIN/) {
      eval "$name $cr->[1]";
      if($@) {
        R_die(COMPILE_FAILURE, "Unable to compile transported BEGIN ($name). eval: $@");
      }
      next;
    }
    my $subref = eval "sub $cr->[1];";
    if($@) {
      R_die(COMPILE_FAILURE, "Unable to compile transported sub ($name). eval: $@");
    }
    *{$name} = $subref;
  }
}

$| = 1;

R_print('READY');
my @args = R_read();
R_print('ACK');
$SIG{__DIE__} = sub { die @_ if $^S; R_die(NATIVE_DEATH, @_); };
R_exit(
  R_main(
    @args
  )
);

1;
EOF
  $cnt =~ s/__CODE__/$code/;
  return $cnt;
}

sub R_read {

}

sub R_write {

}

sub R_die {
  die @_;
}

sub R_print {
}

sub R_exit {
  my $e = shift;
  print Dumper(@_);
  exit($e);
}

sub DESTROY {
  my $s = shift;
  waitpid $s->{ssh_pid}, 0 if(defined($$s{ssh_pid}));
}


1;
# ###########################################################################
# End RObj package
# ###########################################################################

package pdb_packer;
use strict;
use warnings FATAL => 'all';

use DBI;
use Getopt::Long qw(:config no_ignore_case);
use Pod::Usage;
use DateTime;
use Data::Dumper;
use Sys::Hostname;
use DateTime;
$Data::Dumper::Indent = 0;


our $VERSION = 0.036;

use constant DEFAULT_LOG => "/dev/null";
use constant DEFAULT_DATE_FORMAT => "_%Y%m%d";

my $logfile  = DEFAULT_LOG;
my $age      = 0;

# These are 'our' so that testing can fiddle with them easily.
my $pretend  = 0;
my $pack     = 0;
my $rotate   = 0;
my $age_format = "_%Y%m%d";
my $rotate_format = '';
my $cur_date = DateTime->now( time_zone => 'local' )->truncate( to => 'day' );
my $force    = 0;
my $force_small = 0;

sub main {
  # Overwrite ARGV with parameters passed here
  # This means, you must save ARGV before calling this
  # if you want to have ARGV later.
  @ARGV = @_;
  my @DSNs = ();
  my $dsnp = DSNParser->default();
  my $pl;
  $dsnp->mand_key('h', 1);
  $dsnp->mand_key('D', 1);
  $dsnp->mand_key('sU', 1);
  $dsnp->add_key('r', { 'mandatory' => 0, 'desc' => 'Table name prefix' });
  $dsnp->add_key('rF', { 'mandatory' => 0, 'desc' => 'Remote my.cnf' });
  GetOptions(
    "help" => sub {
      pod2usage( -verbose => 1 );
    },
    "pretend|p" => \$pretend,
    "logfile=s" => \$logfile,
    "rotate" => \$rotate,
    "pack" => \$pack,
    "age=s" => \$age,
    "age-format=s" => \$age_format,
    "rotate-format=s" => \$rotate_format,
    "force" => \$force,
    "force-small" => \$force_small,
  );

  unless(scalar @ARGV >= 1) {
    pod2usage(-message => 'Need at least one DSN to operate on', -verbose => 1);
  }

  if($age and $age =~ /(\d+)([wmyd])/i) {
    my %keys = ( w => 'weeks', m => 'months', y => 'years', d => 'days' );
    $age = DateTime::Duration->new( $keys{$2} => $1 );
    $age = $cur_date - $age;
  }
  elsif($age) {
    pod2usage(-message => 'Age: "' . $age . '" does not match format.',
      -verbose => 1);
  }

  $pl = ProcessLog->new($0, $logfile);
  $pl->i("pdb-packer v$VERSION build GIT_SCRIPT_VERSION");

  # Shift off the first DSN, parse it,
  # and then make some keys non-mandatory.
  # The remaining DSNs will fill in from this one.
  push(@DSNs, $dsnp->parse(shift(@ARGV)));
  $dsnp->mand_key('D', 0);
  $dsnp->mand_key('sU', 0);

  # Parse remaining DSNs and fill in any missing values.
  for(@ARGV) {
    push(@DSNs, $dsnp->parse($_));
    $DSNs[-1]->fill_in($DSNs[0]);
  }

  for(@DSNs) {
    if($age and $age_format ne 'createtime' and !$_->has('r')) {
      $pl->e('DSN:', $_->str(), 'is missing required key', 'r',
        'for --age-format');
      return 1;
    }
    unless($_->has('t') or $_->has('r')) {
      $pl->e('DSN:', $_->str(), 'is missing one of the required keys: t or r');
      return 1;
    }
    if($_->has('r') and $_->get('r') !~ /\(.*?\)/) {
      $pl->e('DSN:', $_->str(), 'r key does not have a capture group.');
      return 1;
    }
    if($_->has('t') and $_->has('r')) {
      $pl->e('DSN:', $_->str(), 'has both t and r. You must use only one.');
      return 1;
    }
  }

  foreach my $d (@DSNs) {
    my $dbh = $d->get_dbh(1);
    my @tbls = @{get_tables($d)};
    $pl->m('Working Host:', $d->get('h'), ' Working DB:', $d->get('D'));
    $pl->d('tables:', join(',', @tbls) );
    my ($status, $cfg) = @{MysqlInstance->remote($d, 'config', $d->get('rF'))};
    $pl->d('status:', $status, 'cfg:', $cfg);
    unless($status eq 'EXIT') {
      $pl->e($d->get('h'), "did not return host config correctly. Got:", Dumper($cfg));
      next;
    }
    $pl->d('Host config:', Dumper($cfg));
    $pl->d('Host datadir:', $cfg->{'mysqld'}->{'datadir'});
    foreach my $t (@tbls) {
      # Set the table key in the DSN
      # Rather than making a new DSN for each table, we just
      # overwrite the key - saves space.
      $d->{'t'}->{'value'} = $t;
      my $r;
      if($age and table_age($d) and table_age($d) > $age ) {
        $pl->m('Skipping:', $t, 'because it is newer than', $age);
        next;
      }
      $pl->m('Operating on:', $t);
      if($rotate) {
        $r = undef;
        my $tr = TableRotater->new(
          $d,
          $rotate_format || DEFAULT_DATE_FORMAT,
          $dbh
        );
        my $ta = TableAge->new($d->get_dbh(1),
          ($d->get('r') || $d->get('t')) . ($rotate_format || DEFAULT_DATE_FORMAT));
        my $age = $ta->age_by_name($d->get('t'));
        if( $age ) {
          $pl->m('  Table looks already rotated for', $age);
        }
        else {
          $pl->m('  Rotating', 'to',
            $tr->date_rotate_name(
              $d->get('t'),
              $cur_date
            )
          );
          eval {
            # This modifies the table name in the DSN
            # So that the pack operation will work on the rotated name
            # if a rotation happened.
            if($tr->table_for_date($d->get('D'), $d->get('t'), $cur_date)) {
              $pl->m('  ..Table already rotated for', $cur_date);
              $d->{'t'}->{'value'} = $tr->date_rotate_name(
                $d->get('t'), $cur_date);
              $t = $d->get('t');
              die('Already rotated');
            }
            else {
              $d = rotate_table($tr, $d) unless($pretend);
              $t = $d->get('t');
            }
          };
          if($@ and $@ =~ /^Unable to create new table (.*?) at/) {
            $pl->e('  ..There was an error creating the replacement table.');
            $pl->e('  ..It is advised to manually examine the situation.');
            $pl->e(' .. DSN:', $d->str() ."\n",
              ' .. Temp table name:', $1);
            $pl->e('Exception:', $@);
            return 1;

          }
          elsif($@ and $@ =~ /^Failed to rename table to (.*), (.*) at/) {
            $pl->e('  ..There was an error renaming the tables.');
            $pl->e('  ..You must manually examine the situation.');
            $pl->e('  ..DSN:', $d->str() ."\n",
              ' ..Temp table name:', $2 . "\n",
              ' ..New table name:', $1);
            $pl->e('Exception:', $@);
            return 1;
          }
          elsif($@ and $@ =~ /^Already rotated/) { $pl->d('Redoing age evaluation.'); redo; }
          elsif($@) {
            $pl->e('Unknown exception:', $@);
          }
          else {
            $pl->m('  ..Rotated successfully.') unless($pretend);
            redo;
          }
        } # Else, table not rotated
      }
      if($pack) {
        $r = undef;
        $pl->m('  MyISAM Packing');
        eval {
          $r = pack_table($cfg->{'mysqld'}->{'datadir'}, $d, $t) unless($pretend);
        };
        $pl->d('Pack result:', 'Out:', $r->[0], 'Code:', $r->[1], 'Eval:', $@);
        if($r and $r->[0] and $r->[0] =~ /already/) {
          $pl->d('  ..table already compressed.');
        }
        elsif($r and $r->[0] and $r->[0] =~ /error/i) {
          $pl->m('  ..encountered error.');
          $pl->e(' ', $r->[0], 'code:', $r->[1]);
        }
        elsif($@) {
          $pl->m('  ..encountered fatal error.');
          $pl->e(' ', $@);
          return 1;
        }
        elsif(!$pretend and $r) {
          $pl->m('  ..OK');
        }
      }
    }
  }

  return 0;
}

sub get_tables {
  my ($dsn) = @_;
  my $schema = $dsn->get('D');
  my $sql;
  my $regex;
  if($dsn->get('t')) {
    $sql = qq|SHOW TABLES FROM `$schema` LIKE '|. $dsn->get('t') ."'";
    $regex = $dsn->get('t');
  }
  elsif($dsn->get('r')) {
    $sql = qq|SHOW TABLES FROM `$schema`|;
    $regex = $dsn->get('r');
  }
  my @tbls = grep /^$regex$/,
  map { $_->[0] } @{$dsn->get_dbh(1)->selectall_arrayref($sql)};
  return \@tbls;
}

sub table_age {
  my ($dsn) = @_;
  my $ta = TableAge->new($dsn->get_dbh(1), $age_format);
  if($age_format eq 'createtime') {
    return $ta->age_by_status($dsn->get('D'), $dsn->get('t'));
  }
  else {
    my $reg = $dsn->get('r');
    return $ta->age_by_name(($dsn->get('t') =~ /^$reg$/));
  }
}

sub rotate_table {
  my ($tr, $dsn) = @_;
  $dsn->{'t'}->{'value'} = $tr->date_rotate(
    $dsn->get('D'),
    $dsn->get('t'),
    $cur_date
  );
  return $dsn;
}

sub pack_table {
  my ($datadir, $dsn) = @_;

  my $tp = TablePacker->new($dsn, $datadir);
  # If the table is not a myisam table - we convert it.
  if($tp->engine() ne 'myisam') {
    $tp->mk_myisam($0 . ' on ' . hostname());
  }
  if($tp->engine() eq 'myisam' and $tp->format() eq 'compressed') {
    unless($force) {
      return [$dsn->get('t') .' is already compressed.', 0];
    }
  }
  if($dsn->get('h') ne 'localhost') {
    my $ro = RObj->new($dsn);
    # Make sure the RObj has the needed modules
    $ro->add_use('TablePacker', 'DBI');
    $ro->add_package('DSN');
    $ro->add_package('Which');
    $ro->add_package('TablePacker');
    $ro->add_main(sub {
        # This packs and checks the table specified by $dsn
        my ($self, $force_small) = @_;
        eval {
          local $SIG{__DIE__};
          $self->pack($force_small);
          $self->check();
        };
        return $self;
      });
    $tp = [$ro->do($tp, $force_small)]->[1];
  }
  else {
    eval {
      $tp->pack($force_small);
      $tp->check();
    };
  }
  print( STDERR 'TablePacker: ', Dumper($tp));
  # Flush the table so that mysql reloads the .FRM file.
  $tp->flush();
  chomp($tp->{errstr}) if($tp->{errstr});
  return [$tp->{errstr}, $tp->{errval}];
}

if(!caller) { exit(main(@ARGV)); }

1;

=pod

=head1 NAME

pdb-packer - Rotate and Compress tables.

=head1 SYNOPSIS

pdb-packer [options] DSN ...

Each set of tables specified by each DSN will be packed
in the order they are listed.

=head1 DSN

A Maatkit style DSN.

Keys: h,u,p,F,sU,sK,rF,r,t,D

  h - host
  u - mysql user
  p - mysql password
  F - mysql defaults file
  sU - ssh user
  sK - ssh key
  rF - remote mysql defaults file
  r - table regex
  t - table name
  D - schema

The C<'r'> key is a perl regex to match table names against.
It MUST have exactly one capture group which selects exactly
the L<--age-format> portion of the table name for when L<--age> is used.

Example:

  table_name(_\d+)

=head1 OPTIONS

=over 4

=item B<--pretend>

Don't actually do anything. Just report what would happen.

=item B<--logfile=path>

Where to write out logfile. Normally messages just go to the console
and then are thrown away. However, by specifying the path to a file, or a
string starting such as syslog:LOCAL0 it's possible to also log to a
file or syslog.

Default: /dev/null

=item B<--rotate>

If passed, then the named/matched tables will be renamed to include a datestamp.

Default: (off)

=item B<--age>

Tables older than B<--age> will be operated on.

You can specify a string like: C<XX[dwmy]> where XX is a number,
C<d> is days, C<w> is weeks, C<m> is months, and C<y> is years.

The suffixes are case-insensitive.

Examples:

  --age 4d  # Tables older than 4 days
  --age 1m  # Tables older than 1 month
  --age 2W  # Tables older than 2 weeks

Default: (none)

=item B<--rotate-format>

The value is the format to append to the table name. If no value
is passed, or if an empty value is used, then it defaults to:
C<'_%Y%m%d'> (4-digit year, 2 digit month, 2 digit day).

See C<strftime(3)> for all possible formatting codes.

Example:
    table name: testbl
    rotated name: testbl_20100317

It's not currently possible to post or pre-date tables
- rotation always dates based on the time when the tool started.
So, even if the tool starts at 23:59 on 2010-03-17 and runs till
01:01 on 2010-03-18, all rotated tables will be dated 2010-03-17.

Default: C<_%Y%m%d>

=item B<--age-format>

Selects the method for determining table age.

There are two different methods for determining when a table was created.

=over 8

=item createtime

This is the C<Created_At> property of a table as recorded by MySQL.
Unfortunately, this property is reset if an C<ALTER TABLE> is done,
even a trivial one. Which is why this is not the default.

=item datestamp

This method uses a datestamp after the table name.

It defaults to: C<'_%Y%m%d'> so that C<'testtbl_20100317'> is interpreted
to have been created on 03/17/2010 at 00:00:01.

This method is the default.

=back

This option only needs to be specified when L<--age> is specified and
the default format is insufficient for some reason. To use the datestamp
method the value of this option should be a string with C<strftime(3)> flags.
To use the createtime method, the value of this option should be C<'createtime'>.

Default: C<_%Y%m%d>

=item B<--pack>

If passed, then, matched tables will be converted to packed myisam tables.

Default: off

=item B<--force>

Force packing to run, even if mysql thinks the table is already packed.


=item B<--force-small>

Force packing to run even if the table is too small.

=back

=cut
