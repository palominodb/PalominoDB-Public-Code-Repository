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
# Path package fce50b360e525d020cafb366ba404a74b1fbddb5
# ###########################################################################
package Path;
use File::Find;

sub dir_empty {
  my $dir = shift;
  eval "use File::Find;";
  my $rmtree_sub = sub {
    if(-d $File::Find::name && $File::Find::name ne $dir) {
      rmdir $File::Find::name or die('rmtree: unable to remove directory '. $File::Find::name);
    }
    elsif($_ ne $dir) {
      unlink $File::Find::name or die('rmtree: unable to delete file '. $File::Find::name);
    }
    elsif($_ eq $dir) {
      return;
    }
    else {
      die('rmtree: unexpected error when attempting to remove ' . $File::Find::name);
    }
  };
  find( { wanted => $rmtree_sub, no_chdir => 1, bydepth => 1 }, $dir );

  return 0;
}
1;
# ###########################################################################
# End Path package
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
# MysqlMasterInfo package 11777931067b2300189d297d99d94c99f48c338e
# ###########################################################################
package MysqlMasterInfo;
use strict;
use warnings FATAL => 'all';

sub open {
  my ($class, $path) = @_;
  my $self = {};
  my $fh;
  $self->{path} = $path;
  CORE::open($fh, '<', $path) or return $!;
  $self->{lines} = ();
  chomp(@{$self->{lines}} = <$fh>);
  close($fh);

  return bless $self, $class;
}

sub write {
  my ($self, $path) = @_;
  my $write_path = ($path || $self->{path});
  my $fh;
  CORE::open($fh, '>', $write_path) or return undef; 
  {
    local $, = "\n";
    print $fh @{$self->{lines}};
  }
  close($fh);
  return 0;
}

sub log_file {
  my $self = shift;
  return $self->_update(1, qr/[^\0]+/, @_);
}

sub log_pos {
  my $self = shift;
  return $self->_update(2, qr/\d+/, @_);
}

sub master_host {
  my $self = shift;
  return $self->_update(3, qr/.+/, @_);
}

sub master_user {
  my $self = shift;
  return $self->_update(4, qr/.+/, @_);
}

sub master_password {
  my $self = shift;
  return $self->_update(5, qr/.*/, @_);
}

sub master_port {
  my $self = shift;
  return $self->_update(6, qr/\d+/, @_);
}

sub connect_retry {
  my $self = shift;
  return $self->_update(7, qr/\d+/, @_);
}

sub master_ssl_allowed {
  my $self = shift;
  return $self->_update(8, qr/0|1/, @_);
}

sub master_ssl_ca_file {
  my $self = shift;
  return $self->_update(9, qr/[^\0]*/, @_);
}

sub master_ssl_ca_path {
  my $self = shift;
  return $self->_update(10, qr/[^\0]*/, @_);
}

sub master_ssl_cert {
  my $self = shift;
  return $self->_update(11, qr/[^\0]*/, @_);
}

sub master_ssl_cipher {
  my $self = shift;
  return $self->_update(12, qr/[\w\-_]*/, @_);
}

sub master_ssl_key {
  my $self = shift;
  return $self->_update(13, qr/[^\0]*/, @_);
}

sub master_ssl_verify_server_cert {
  my $self = shift;
  return $self->_update(14, qr/0|1/, @_);
}

sub _update {
  my ($self, $lineno, $filter, $new) = @_;
  my $old = $self->{lines}->[$lineno];
  if(defined($new) and $new =~ $filter) {
    $self->{lines}->[$lineno] = ($new || $old);
  }
  return $old;
}

1;

# ###########################################################################
# End MysqlMasterInfo package
# ###########################################################################

# ###########################################################################
# MysqlSlave package 8866494994be6e9f0ded90b27e985b1fdc7bf7aa
# ###########################################################################
package MysqlSlave;
use strict;
use warnings FATAL => 'all';
use Carp;

sub new {
  my ($class, $dsn) = @_;
  my $self = {};
  $self->{dsn} = $dsn;
  return bless $self, $class;
}

sub read_only {
  my ($self, $value) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  if(defined($value)) {
    croak('value must be 0 or 1') unless( $value eq '0' or $value eq '1' );
    $dbh->do('SET GLOBAL read_only = '. int($value));
  }
  return $dbh->selectcol_arrayref('SELECT @@read_only')->[0];
}

sub auto_inc_inc {
  my ($self) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  return $dbh->selectcol_arrayref('SELECT @@auto_increment_increment')->[0];
}

sub auto_inc_off {
  my ($self) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  return $dbh->selectcol_arrayref('SELECT @@auto_increment_offset')->[0];
}

sub master_status {
  my ($self) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  my ($log_file, $log_pos) = $dbh->selectrow_array('SHOW MASTER STATUS');

  return wantarray ? ($log_file, $log_pos) : $log_file ? 1 : 0;
}

sub slave_status {
  my ($self) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  return $dbh->selectrow_hashref(q|SHOW SLAVE STATUS|);
}

sub flush_logs {
  my ($self) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  return $dbh->do('FLUSH LOGS');
}

sub start_slave {
  my ($self, $master_log_file, $master_log_pos) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  if($master_log_file and $master_log_pos) {
    $master_log_file = $dbh->quote($master_log_file);
    return $dbh->do("START SLAVE UNTIL MASTER_LOG_FILE=$master_log_file, MASTER_LOG_POS=" . int($master_log_pos));
  }
  return $dbh->do('START SLAVE');
}

sub stop_slave {
  my ($self) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  return $dbh->do('STOP SLAVE');
}

sub change_master_to {
  my ($self, @args) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  my %master_keys = (
    MASTER_HOST => 1,
    MASTER_USER => 1,
    MASTER_PASSWORD => 1,
    MASTER_PORT => 1,
    MASTER_LOG_FILE => 1,
    MASTER_LOG_POS => 1,
    MASTER_SSL => 1,
    MASTER_SSL_CA => 1,
    MASTER_SSL_CAPATH => 1,
    MASTER_SSL_CERT => 1,
    MASTER_SSL_KEY => 1,
    MASTER_SSL_CIPHER => 1,
    MASTER_CONNECT_RETRY => 1,
    MASTER_SSL_VERIFY_SERVER_CERT => 1
  );
  my %dsn_to_master = (
    'h' => 'MASTER_HOST',
    'u' => 'MASTER_USER',
    'p' => 'MASTER_PASSWORD',
    'P' => 'MASTER_PORT',
    'SSL_key' => 'MASTER_SSL_KEY',
    'SSL_cert' => 'MASTER_SSL_CERT',
    'SSL_CA' => 'MASTER_SSL_CA',
    'SSL_CA_path' => 'MASTER_SSL_CAPATH',
    'SSL_cipher' => 'MASTER_SSL_CIPHER'
  );

  my $sql = 'CHANGE MASTER TO ';
  my %keys = ();
  if(ref($args[0]) and ref($args[0]) eq 'HASH') {
    %keys = %{$args[0]};
  }
  elsif(ref($args[0]) and ref($args[0]) eq 'DSN') {
    my $dsn = shift @args;
    my %args = @args;
    foreach my $k (keys %$dsn) {
      if(exists $dsn_to_master{$k}) {
        $keys{ $dsn_to_master{$k} } = $dsn->get($k) if($dsn->has($k));
      }
    }
    for(keys %args) {
      $keys{$_} = $args{$_};
    }
  }
  else {
    %keys = @args;
  }
  for(keys %keys) {
    croak("Invalid option $_") unless( exists($master_keys{uc($_)}) );
    if($keys{$_} =~ /^\d+$/) {
      $sql .= uc($_) . '=' . $keys{$_} . ', ';
    }
    else {
      $sql .= uc($_) . '=' . $dbh->quote($keys{$_}) . ', ';
    }
  }
  chop($sql);
  chop($sql);
  return $dbh->do($sql);
}


1;
# ###########################################################################
# End MysqlSlave package
# ###########################################################################

package ReMysql;
use strict;
use warnings FATAL => 'all';
use Data::Dumper;
$Data::Dumper::Indent = 0;
use Carp;


my $pid_check_sleep = 10;
my $stop_timeout = 3;
my $start_timeout = 3;
my $slave_timeout = 3;

sub new {
  my ($class, $dry_run, $sandbox_path, $dsn) = @_;
  my $self = {};
  $$self{dsn} = $dsn;
  $$self{dry_run} = $dry_run;
  $$self{sandbox_path} = $sandbox_path;
  $$self{save_mysql} = 1;
  bless $self, $class;

  $self->verify_permissions();

  return $self;
}

sub _ro() {
  my $self = shift;
  $$self{ro} ||= RObj->new($$self{dsn}->get('h'),
    $$self{dsn}->get('sU'), $$self{dsn}->get('sK'));
  return $$self{ro}->copy;
}

sub verify_permissions {
  my ($self) = @_;
  $self->verify_ssh();
  my $config = MysqlInstance->remote($$self{dsn}, 'config')->[-1];
  $$self{config} = $config;

  my $ro = $self->_ro;
  $ro->add_main(\&verify_user_permissions);
  $::PLOG->d($$self{dsn}->get('h').':', 'Preflight: ssh user permissions');
  my @r = $ro->do($config);
  if($r[0] ne 'EXIT') {
    croak($r[0]);
  }
  unless($r[1] == 1) {
    croak('Invalid permissions on remote');
  }
  $self->verify_mysql_permissions();

  return 1;
}

sub verify_ssh {
  my $self = shift;
  my $host = $$self{dsn};
  my $ro = $self->_ro;
  $::PLOG->d($$self{dsn}->get('h').':', 'Preflight: ssh');
  $ro->add_main(sub { return 'OK' });
  my @r = $ro->do();
  unless($r[1] eq 'OK') {
   croak('Unable to ssh to remote');
  }
  return 1;
}

sub verify_user_permissions {
  my $cfg = shift;
  my $r = {};

  my $pid = ($$cfg{'mysqld'}{'pid-file'} || $$cfg{'mysqld_safe'}{'pid-file'});
  die('No pid-file entry in my.cnf') unless($pid);
  die('No datadir entry in my.cnf') unless($$cfg{'mysqld'}{'datadir'});

  open PID_FILE, '<', $pid or die('Unable to open or read pid file');
  chomp($pid = <PID_FILE>);
  close(PID_FILE);

  my @dirs = (
    $$cfg{'mysqld'}{'datadir'},
    '/proc/'. $pid
  );

  for(@dirs) {
    die('Directory "'. (defined($_) ? $_ : 'undef') .'" does not exist') unless( defined($_) && -d $_ );
    my @st = stat($_);
    my ($uid, $gid, $mode) = @st[4,5,2];
    die('User does not own "'. $_ .'"') unless($< == 0 or $uid == $<);
  }

  die('path /tmp/mysql exists') if(-e '/tmp/mysql');

  return 1;
}

sub verify_mysql_permissions {
  my $self = shift;
  $::PLOG->d($$self{dsn}->get('h').':', 'Preflight: verify mysql permissions');
  $$self{dsn}->get_dbh(1);
  my ($gstr) = $$self{dsn}->get_dbh(1)->selectrow_array('SHOW GRANTS');
  if($gstr !~ /SUPER/ and $gstr !~ / ALL /) {
    croak('mysql user needs SUPER');
  }
  return 1;
}

sub make_slave_of {
  my ($self, $master, $user, $pw) = @_;
  $master = $$master{dsn};
  my $ms1 = MysqlSlave->new($master);
  my $ms2 = MysqlSlave->new($$self{dsn});
  my ($m1_file, $m1_pos) = $ms1->master_status();
  my ($binlog_base) = ($m1_file =~ /^(.+)\.\d+$/);
  $::PLOG->d($$self{dsn}->get('h').':', 'making slave of', $master->get('h'));
  $ms2->stop_slave();
  $ms2->change_master_to(
    master_host => $master->get('h'),
    master_user => $user,
    master_port => $master->get('P') || 3306,
    master_password => $pw,
    master_log_file => sprintf("$binlog_base.%06d", 1),
    master_log_pos  => 4
  );
  $ms2->start_slave();
  my $i = 0;
  my $status = defined($ms2->slave_status()->{'Seconds_Behind_Master'}
    ? 1 : 0);
  while($i < 3 and !$status) {
    $status = defined($ms2->slave_status()->{'Seconds_Behind_Master'}
      ? 1 : 0);
    sleep(1);
  }
  continue {
    $i++;
  }
  unless($status) {
    croak('Slave not running after 3 seconds');
  }
}

sub copy_data {
  my ($self, $sandbox_path) = @_;
  my $hostname = $$self{dsn}->get('h');
  my $key = $$self{dsn}->get('sK');
  my $user = $$self{dsn}->get('sU');
  my $datadir = $$self{config}{'mysqld'}{'datadir'};
  $::PLOG->d($$self{dsn}->get('h').':', 'copying data');
  system('scp',
    '-B', '-C', '-r',
    '-p', '-q',
    $key ? ('-i', $key) : (),
    <$sandbox_path/data/*>,
    "$user\@${hostname}:$datadir"
  );
  return $? >> 8;
}

sub check_mysql_pid {
  my $cfg = shift;
  my $pid;
  eval {
    open PID_FILE, '<', $$cfg{'mysqld'}{'pid-file'} or return -1;
    chomp($pid = <PID_FILE>);
  };
  if(defined $pid and -d '/proc/'. $pid ) {
    return 1;
  }
  return 0;
}

sub rebuild_remote {
  my $params = shift;
  my $start_timeout = $$params{'start_timeout'};
  my $stop_timeout = $$params{'stop_timeout'};
  my $pid_check_sleep = $$params{'pid_check_sleep'};
  my $save_mysqldb = $$params{'save_mysqldb'};
  my $mi = MysqlInstance->new();
  my $cfg = $mi->config;
  my $datadir = $$cfg{'mysqld'}{'datadir'};
  my $i=0;

# #############################################################################
# Stop mysql
# #############################################################################

  $mi->stop;
  while($i < $stop_timeout && check_mysql_pid($cfg) > 0) {
    sleep($pid_check_sleep);
    $i++;
  }
  if($i == $stop_timeout) {
    die('mysql did not stop in a timely fashion');
  }
  $i = 0;

# #############################################################################
# Save mysql db to /tmp
# #############################################################################

  system('mv', $datadir . '/mysql', '/tmp/');

# #############################################################################
# Remove existing data
# #############################################################################

  Path::dir_empty($datadir);

# #############################################################################
# print status (removed datadir), and wait for signal (continue)
# #############################################################################

  R_print('datadir ready');
  my @r = R_read();
  unless($r[0] eq 'continue') {
    die('Received invalid signal from controller');
  }

# #############################################################################
# Remove ib_logfiles
# #############################################################################

  unlink($datadir . "/ib_logfile0");
  unlink($datadir . "/ib_logfile1");

# #############################################################################
# Remove new mysql db and restore old from /tmp
# #############################################################################

  system('rm', '-rf', $datadir . '/mysql');
  system('mv', '/tmp/mysql', $datadir . '/');

# #############################################################################
# Start mysql
# #############################################################################

  $mi->start;
  while($i < $start_timeout && !check_mysql_pid($cfg)) {
    sleep($pid_check_sleep);
    $i++;
  }
  if($i == $start_timeout) {
    die('mysql did not start in a timely fashion');
  }

  return 0;
}

sub rebuild {
  my $self = shift;
  my $ro = $self->_ro;
  $ro->add_package('IniFile');
  $ro->add_package('MysqlInstance::Methods');
  $ro->add_package('MysqlInstance');
  $ro->add_package('Path');
  $ro->add_sub('check_mysql_pid', \&check_mysql_pid);
  $ro->add_main(\&rebuild_remote);

  $ro->debug('localhost:9999');

  $ro->start({
      start_timeout => $start_timeout,
      stop_timeout => $stop_timeout,
      pid_check_sleep => $pid_check_sleep,
      save_mysqldb => $$self{'save_mysql'}
    });

  my @r = $ro->read();

  unless($r[0] eq 'datadir ready') {
    die('Got invalid signal from remote end: '. $r[0]);
  }
  else {
    $self->copy_data($$self{sandbox_path});
    $ro->write('continue');
  }
  $ro->wait();
  return 0;
}

1;

package PdbMaster;
use strict;
use warnings FATAL => 'all';

our $VERSION = 0.01;
use Getopt::Long qw(:config permute no_ignore_case);
use Pod::Usage;
use POSIX ':sys_wait_h';
use Data::Dumper;
use Carp;



my $pl;
my $dry_run = 0;
my $sandbox_path;


sub main {
  @ARGV = @_;
  my $dsnp = DSNParser->default();
  my (%o, @hosts);
  $o{'logfile'} = 'pdb-test-harness';
  {
    no warnings 'once';
    $ReMysql::start_timeout = 120;
    $ReMysql::stop_timeout = 120;
  }
  GetOptions(\%o,
    'help|h',
    'dry-run|n',
    'logfile|L=s',
    'repl-user=s',
    'repl-password=s',
    'no-fork'
  );
  if(scalar @ARGV < 2) {
    pod2usage(-message => "Must have a sandbox and at least two DSNs",
      -verbose => 1);
  }
  $dry_run = $o{'dry-run'};
  $sandbox_path = shift @ARGV;
  @hosts        = @ARGV;
  if(! -d $sandbox_path or ! -f "$sandbox_path/my.sandbox.cnf" ) {
    pod2usage(-message => "First argument must be a sandbox directory.",
      -verbose => 1);
  }

  $pl = ProcessLog->new($0, $o{'logfile'}, undef);
  {
    no strict 'refs';
    no warnings 'once';
    *::PLOG = \$pl;
  }
  @hosts = map { $dsnp->parse($_) } @hosts;
  $pl->i("pdb-master v$VERSION build SCRIPT_GIT_VERSION");
  foreach my $host (@hosts) {
    my $host_cfg;
    eval {
      $host = ReMysql->new($dry_run, $sandbox_path, $host);
    };
    if($@) {
      chomp($@);
      $pl->e('Error in pre-flight check for host:', $host->get('h'));
      $pl->e('Error:', $@);
      return 1;
    }
  }

  $pl->i('All pre-flight checks passed. Beginning work.');
  unless($o{'no-fork'}) {
    my @pids;
    foreach my $host (@hosts) {
      push @pids, spawn_worker($host);
      $pl->d('Process:', $pids[-1], 'started.');
    }

    my $kid;
    while( ($kid = waitpid(-1, 0)) >= 0 ) {
      # If there was an error with any of the workers,
      # kill them all!
      $pl->d('Return code:', ($? >> 8));
      if( ($? >> 8) > 0 ) {
        for(@pids) {
          kill(15, $_); # Send SIGTERM
        }
        $pl->e('One of the workers encountered an error.');
        return 1;
      }
      else {
        $pl->d('Process:', $kid, 'completed.');
      }
    }
  }
  else {
    $pl->m('Rebuilding serially due to --no-fork');
    foreach my $host (@hosts) {
      $pl->d('doing:', $host->{dsn}->get('h'));
      $host->rebuild();
    }
  }

  $pl->d('All hosts prepped for re-slaving.');
  $hosts[0]->make_slave_of($hosts[1], $o{'repl-user'}, $o{'repl-password'});
  $hosts[1]->make_slave_of($hosts[0], $o{'repl-user'}, $o{'repl-password'});

  foreach my $host (@hosts[2 .. $#hosts]) {
    $host->make_slave_of($hosts[0], $o{'repl-user'}, $o{'repl-password'});
  }
  $pl->i('pdb-master finished.');
  return 0;
}

sub spawn_worker {
  my $host = shift;
  my $pid = fork();
  if(not defined($pid)) {
    croak("fork failed [$!]");
  }
  if($pid) {
    return $pid;
  }
  exit($host->rebuild());
}


if(!caller) { exit(main(@ARGV)); }

1;
