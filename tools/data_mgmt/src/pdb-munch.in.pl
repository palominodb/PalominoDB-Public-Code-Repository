#!/usr/bin/env perl
# Copyright (c) 2009-2010, PalominoDB, Inc.
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
# TableIndexes package e331bbb6f2668fa9aea607f72f03e93f0390ff37
# ###########################################################################
package TableIndexes;
use strict;
use warnings FATAL => 'all';
use Carp;

sub new {
  my ($class, $dsn) = @_;
  my $self = {};

  $self->{dsn} = $dsn;

  return bless $self, $class;
}


sub indexes {
  my ($self, $db, $table) = @_;
  my $dbh = $$self{dsn}->get_dbh(1);
  my ($indexes, $columns);

  if(not defined $table) {
    ($db, $table) = split /\./, $db;
  }

  $indexes = $dbh->selectall_arrayref("SHOW INDEXES FROM `$db`.`$table`", { Slice => {} });
  foreach my $col (@{$dbh->selectall_arrayref("SHOW COLUMNS FROM `$db`.`$table`", { Slice => {} });}) {
    $columns->{$col->{Field}} = $col->{Type};
  }

  $indexes = [
    map {
      my $key_name = $_->{'Key_name'};
      my $col_name = $_->{'Column_name'};
      my $col_type = $columns->{$col_name};
      $col_type =~ s/\(\d+\)//;
      $col_type = lc($col_type);
      my $key_type = undef;
      if($key_name ne 'PRIMARY') {
        if($_->{'Non_unique'}) {
          $key_type = 'key';
        }
        else {
          $key_type = 'unique';
        }
      }
      else {
        $key_type = 'primary';
      }
      { 'name' => $_->{'Key_name'}, 'column' => $_->{'Column_name'}, 'key_type' => $key_type, 'column_type' => $col_type }
    } @$indexes
  ];

  return $indexes;
}




sub i_col_typ {
  my $x = shift;
  my $i = 0;

  my @index_priority = ('primary-int', 'primary-timestamp',
                      'unique-int', 'unique-timestamp',
                      'key-int', 'key-timestamp');
  my $kt = $x->{'key_type'};
  my $ct = $x->{'column_type'};

  $i++ while($index_priority[$i] and $index_priority[$i] !~ /^${kt}-${ct}$/);
  return -1 if(! $index_priority[$i]);
  return $i;
}

sub sort_indexes {
  my ($self, $db, $table) = @_;
  my $dbh = $$self{dsn}->get_dbh(1);
  my ($indexes, $columns);

  if(not defined $table) {
    ($db, $table) = split /\./, $db;
  }

  $indexes = [
    sort {
       i_col_typ($a) <=> i_col_typ($b);
    }
    grep { i_col_typ($_) >= 0 } @{$self->indexes($db, $table)}
  ];
  croak("No suitable index found") if(!@$indexes);
  return $indexes;
}


sub get_best_index {
  my ($self, $db, $table) = @_;
  my $dbh = $$self{dsn}->get_dbh(1);
  my ($indexes, $columns, @index_type);

  if(not defined $table) {
    ($db, $table) = split /\./, $db;
  }
  return $self->sort_indexes($db, $table)->[0];

}


sub walk_table {
  my ($self, $index, $size, $start, $cb, $db, $table, @cb_data) = @_;
  $start ||= 0;

  if(not defined $table) {
    ($db, $table) = split /\./, $db;
  }
  if(not defined $index) {
    $index = $self->get_best_index($db, $table);
  }

  return ($self->walk_table_base(index => $index, size => $size, db => $db,
                                start => $start, callback => $cb,
                                table => $table, data => [@cb_data]))[0];
}


sub walk_table_base {
  my ($self, %a) = @_;
  my ($rows, $last_idx) = (0, 0);
  my ($idx_col) = ('');
  my $dbh = $self->{dsn}->get_dbh(1);

  for(qw(index callback size db table)) {
    croak("Missing required parameter: $_") unless(exists $a{$_});
  }

  $idx_col = $a{'index'}{'column'};
  $a{'filter_clause'} ||= '1=1';
  $a{'columns'} ||= ['*'];
  if(!ref($a{'columns'})) {
    $a{'columns'} = [$a{'columns'}];
  }


  eval {
    my ($sth, $min_idx, $max_idx, $cb, $row, @data);

    $dbh->{AutoCommit} = 0;
    $cb = $a{'callback'};
    if(exists $a{'data'}) {
      @data = @{$a{'data'}};
    }
    $min_idx = $dbh->selectrow_array("SELECT MIN(`$idx_col`) FROM `$a{'db'}`.`$a{'table'}`");
    $last_idx = $dbh->selectrow_array("SELECT MAX(`$idx_col`) FROM `$a{'db'}`.`$a{'table'}`");
    if(not defined $min_idx or not defined $last_idx) {
      die("No rows, or invalid index column for $a{'db'}.$a{'table'}");
    }
    $min_idx = $a{'start'} if(exists $a{'start'});
    $max_idx = $min_idx+$a{'size'};
    $sth = $dbh->prepare("SELECT ". join(',', @{$a{'columns'}}) .
                         " FROM `$a{'db'}`.`$a{'table'}`".
                         "  WHERE (`$idx_col` >= ? AND `$idx_col` <= ?) ".
                         "   AND ($a{'filter_clause'})");

    do {
      $sth->execute($min_idx, $max_idx);
      if($a{'fetch_bulk'}) {
        my $results = $sth->fetchall_arrayref({ });
        $rows += scalar @$results;
        &$cb($idx_col, $dbh, $min_idx, $max_idx, $results, @data);
      }
      else {
        while($row = $sth->fetchrow_hashref) {
          $rows++;
          &$cb($idx_col, $dbh, $min_idx, $max_idx, $row, @data);
        }
      }
      $min_idx = $max_idx+1;
      $max_idx += $a{'size'};
      if($max_idx > $last_idx) {
        $max_idx = $last_idx;
      }
    } while($min_idx <= $last_idx);
    $dbh->commit;
    $dbh->{'AutoCommit'} = 0;
  };
  if($@) {
    $_ = "$@";
    $dbh->rollback;
    croak($_);
  }

  return ($rows, $last_idx);
}

1;
# ###########################################################################
# End TableIndexes package
# ###########################################################################

package pdb_munch;
use strict;
use warnings FATAL => 'all';

use Getopt::Long qw(:config no_ignore_case);
use Text::CSV_XS;
use Data::Dumper;
use DBI;
use Pod::Usage;


my $default_spec =<<'EOF';
; This is the built-in spec provided with pdb-munch.
; All of the source values have been commented out, and you MUST
; uncomment them and fill them in with real values.

[name]
column-type = varchar
method      = roundrobin
; Uncomment the below if you've got a CSV of firstname,lastname
;source    = csv:names.csv
match1      = (\w+) (\w+)

[phonenumber]
column-type = varchar
method      = random
; Uncomment the below if you've got a CSV of phone numbers
;source     = csv:phone_numbers.csv

; These matches are in descreasing priority
; The capture group specifies which portion of the number
; to replace with seed data.
match1      = \d{3}-(\d{3}-\d{4})
match2      = \(\d{3}\) (\d{3}-\d{4})
match3      = \+?\d{1,3} \d{3} (\d{3} \d{4})
match4      = \+?\d{1,3} \d{3} (\d{3}-\d{4})

[email]
column-type = varchar
method = random
;source = random
; Assumes a pre-sanitized email address
match1 = (.*?)@.*

[address_line_one]
column-type = varchar
method      = random
;source      = csv:addresses_line_one.csv
method      = random

match1     = \s*(\d+) ([a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ?).*
match2     = \s*(\d+) ([a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ?).*
match3     = \s*(\d+) ([a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ?).*
match4     = \s*(\d+) ([a-zA-Z0-9\.]+ ? [a-zA-Z0-9\.]+ ?).*
EOF

my %c;
my %spec;
my %conf;
my $pl;
my $cur_tbl;
my $db;
my $dsn;

my $rr_upd;
my $changed_rows;

# key => table name
# value => [table, id_column, cur_id, min_id, max_id]
my %resume_info;

sub main {
  @ARGV = @_;
  my $dsnp = DSNParser->default();
  my $tbl_indexer;
  %resume_info = ();

  %c = ('logfile' => "$0.log", 'batch-size' => 10_000, 'max-retries' => 1_000);
  %spec = ();
  %conf = ();
  $cur_tbl = undef;
  $dsn = undef;
  $db = undef;
  $rr_upd = 0;
  $changed_rows = 0;
  GetOptions(\%c,
    'help|h',
    'logfile|L=s',
    'dry-run|n',
    'dump-spec',
    'spec|s=s',
    'config|c=s',
    'batch-size|b=i',
    'limit=i',
    'max-retries=i',
    'resume|r=s'
  );

  if($c{'help'}) {
    pod2usage(-verbose => 99);
    return 1;
  }
  
  $pl = ProcessLog->new($0, $c{logfile});
  
  ## Dump the spec and exit, if requested.
  if($c{'dump-spec'}) {
    my $spec_fh;
    open($spec_fh, ">default_spec.conf") or die("Unable to open default_spec.conf for writing");
    print($spec_fh $default_spec);
    close($spec_fh);
    $pl->i("Dumped default spec to default_spec.conf");
    return 0;
  }
  
  if(not exists $c{spec}) {
    $pl->e("--spec required. Try --help.");
    return 1;
  }
  
  if(not exists $c{config}) {
    $pl->e("--config required. Try --help.");
    return 1;
  }
  
  ## Load the spec file into a hash
  %spec = IniFile::read_config($c{spec});
  if(not %spec) {
    $pl->e("Unable to load $c{spec}.");
    return 1;
  }
  
  ## Load the config file into a hash
  ## This is done before loading custom perl modules so that
  ## this data is available when they load.
  %conf = IniFile::read_config($c{config});
  if(not %conf) {
    $pl->e("Unable to load $c{config}");
    return 1;
  }
  
  ## Verify each datatype in the spec.
  foreach my $type (keys %spec) {
    next if ($type =~ /^__\w+__$/); # Skip control sections
    for my $c (qw(source column-type method)) {
      if(not defined($spec{$type}->{$c})) {
        $pl->e("$c is required for all data specs");
        return 1;
      }
    }
  }
  
  ## Load all the CSV and List data sources into the spec, directly.
  ## Load all 'module' sources by "require"ing the associated method.
  foreach my $type (keys %spec) {
    next if ($type =~ /^__\w+__$/); # Skip control sections
    my $src = $spec{$type}->{source};
    $pl->d("src:", $src);
    if($src =~ /^csv:(.*)/) {
      my $fh;
      my $csv = Text::CSV_XS->new({binary => 1});
      unless(open($fh, "<$1")) {
        $pl->e("Unable to open seed data: $1");
        return 1;
      }
      ## Assuming that the data is basically hand-generated and thus
      ## will not be gigantic.
      while(my $row = $csv->getline($fh)) {
        push( @{$spec{$type}->{data}}, $row );
      }
    }
    elsif($src =~ /^list:(.*)/) {
      $spec{$type}->{data} = [map { [$_] }split(/,/, $1)];
    }
    elsif($src =~ /^module:(.*)/) {
      $spec{$type}->{source} = "module";
      require "$1";
    }
  }
  
  ProcessLog::_PdbDEBUG >= 3 && $pl->d("Spec:", Dumper(\%spec));
  ProcessLog::_PdbDEBUG >= 3 && $pl->d("Config:", Dumper(\%conf));
  
  ## Get connection information out of the config file
  $dsn = $dsnp->parse($conf{'__connection__'}{'dsn'});
  $db  = $dsn->get('D');
  delete $conf{'__connection__'};
  $tbl_indexer = TableIndexes->new($dsn);
  
  load_resume($c{resume}) if($c{resume});

  foreach my $tbl (sort keys %conf) {
    $rr_upd = 0;
    $cur_tbl = $tbl;
    $resume_info{$cur_tbl} ||= [undef, undef, undef, undef];
    $pl->d("Table config:", $conf{$tbl});
    $tbl_indexer->walk_table(undef, $c{'batch-size'}, $resume_info{$cur_tbl}->[2], \&update_row, $db, $tbl);
  }

  $pl->i("Changed Rows:", $changed_rows);

  return 0;
}

sub generate_varchar {
  # the length of the random string to generate
  my $length_of_randomstring=shift;
  $length_of_randomstring ||= 5;

  my @chars=('a'..'z','A'..'Z','0'..'9','_');
  my $random_string;
  foreach (1..$length_of_randomstring) {
    # rand @chars will generate a random 
    # number between 0 and scalar @chars
    $random_string.=$chars[rand @chars];
  }
  return $random_string;
}

sub generate_num {
  my $power_up_to=shift;
  my $min_digits=shift;
  $power_up_to=1 if($power_up_to==0);
  $min_digits ||= $power_up_to;
  # $power_up_to == 0 is a number between 0-9
  # == 1 is 0-9
  # == 2 is 0-99
  # == 3 is 0-199
  # == 4 is 0-1999
  # etc.

  return sprintf("%0${min_digits}d", int(rand(10**$power_up_to)));
}

# Saves information about the progress of the muncher to the file
# specified by --resume
sub save_resume {
  my $res_fh;
  open($res_fh, ">$c{resume}") or die("Unable to open $c{resume}: $!");
  foreach my $tbl (sort keys %resume_info) {
    print($res_fh join("\t", ($tbl, @{$resume_info{$tbl}})), "\n");
  }
  close($res_fh);

  return 1;
}

# Loads information about the progress of the muncher from the file
# specified by --resume
sub load_resume {
  my $res_fh;
  my @res;
  open($res_fh, "<$c{resume}") or die("Unable to open $c{resume}: $!");
  while(<$res_fh>) {
    my ($tbl, @data) = split(/\t/);
    chomp(@data);
    $resume_info{$tbl} = [@data];
  }
}

## Does the actual work of updating rows to have obfuscated values.
sub update_row {
  my ($idx_col, $dbh, $min_idx, $max_idx, $row) = @_;
  my $dry_run = $c{'dry-run'};
  my $tbl_config = $conf{$cur_tbl};
  my $max_retries = $c{'max-retries'};
  my $retries = 0;
  ## @vals contains the updated column data after the COLUMN: loop
  ## @data contains the seed data, if any is present.
  my (@vals, $data);

# We jump to this label when there was a duplicate key error on the row
# and $c{'max-retries'} is greater than 0.
UPDATE_ROW_TOP:
  @vals = ();
  $data = [];

  ProcessLog::_PdbDEBUG >= 2 && $pl->d("Row:", "$idx_col >= $min_idx AND $idx_col <= $max_idx", Dumper($row));

  ## The keys are sorted here to force the same order in the query as in @vals
  ## Since, @vals is passed wholesale onto $sth->execute() later.
  my $sth = $dbh->prepare_cached("UPDATE `$db`.`$cur_tbl` SET ". join("=?, ", sort keys %$tbl_config) ."=? WHERE `$idx_col`=?");

  COLUMN: foreach my $col (sort keys %$tbl_config) {
    if(not defined($$row{$col})) {
      $row->{$col} = "";
    }
    ## Populate the @data array with either seed data from the pre-loaded CSV file
    ## Or a couple values from a random string generator.
    if($spec{$$tbl_config{$col}}->{source} eq "random") {
      $data = [ [generate_varchar(int(rand(length($row->{$col}))))],
      [generate_varchar(int(rand(length($row->{$col}))))],
      [generate_varchar(int(rand(length($row->{$col}))))] ];
    }
    elsif($spec{$$tbl_config{$col}}->{source} eq "module") {
      ## Nothing done here. This is to prevent the catch-all from running.
    }
    else {
      $data = $spec{$$tbl_config{$col}}->{data};
    }
    
    ## Select the data in the fashion requested.
    if($spec{$$tbl_config{$col}}->{method} eq 'random') {
      push @vals, $$data[int(rand(scalar(@$data)-1))];
    }
    elsif($spec{$$tbl_config{$col}}->{method} eq 'roundrobin') {
      push @vals, $$data[ $rr_upd % (scalar(@$data)-1) ];
      $rr_upd++;
    }
    elsif($spec{$$tbl_config{$col}}->{source} eq "module") {
      no strict 'refs';
      push @vals, &{$spec{$$tbl_config{$col}}->{method}}($dbh, $row->{$col}, $idx_col, $col, $row);
      # perlsubs called via the module interface
      # should signal that they deleted the row by returning an empty hashref.
      # The code will fall through to the update, which should simply do nothing,
      # since the row is now missing.
      if(ref($vals[-1]) eq 'HASH') {
        last COLUMN;
      }
      next COLUMN;
    }

    ## Keys are sorted here so that each of the matchN keys is in ascending order
    ## thus prioritising lower values of N.
    SEED_KEY: foreach my $sk (sort(grep(/match\d+/, keys(%{$spec{$$tbl_config{$col}}}))) ) {
      my $rgx = $spec{$$tbl_config{$col}}{$sk};
      my @res = $row->{$col} =~ /^$rgx$/;
      ProcessLog::_PdbDEBUG >= 2 && $pl->d("R:", $col, qr/^$rgx$/, $#res+1, @res);
      if(@res) {
        for(my $i=0; $i < $#res+1; $i++) {
          ProcessLog::_PdbDEBUG >= 2 && $pl->d("V:", $col, Dumper(\@vals));
          ProcessLog::_PdbDEBUG >= 2 && $pl->d("S:", $col, $res[$i], "(", @{$vals[-1]}, ")", "*", $vals[-1]->[$i], "*", $i, scalar @{$vals[-1]});
          substr($row->{$col}, index($row->{$col}, $res[$i]), length($res[$i]), $vals[-1]->[$i]);
        }
        $vals[-1] = $row->{$col};
        last SEED_KEY;
      }
    }
    if(ref($vals[-1])) {
      if($spec{'__params__'}{'die-on-unmatched'}) {
        die("Unable to match $col");
      }
      $vals[-1] = $row->{$col};
    }
  }
  ProcessLog::_PdbDEBUG >= 2 && $pl->d("SQL:", "UPDATE `$db`.`$cur_tbl` SET ". join("=?, ", sort keys %$tbl_config) ."=? WHERE `$idx_col`=?");
  ProcessLog::_PdbDEBUG >= 2 && $pl->d("SQL Bind:", @vals, $row->{$idx_col});
  eval {
    $sth->execute(@vals, $row->{$idx_col}) unless($dry_run or ref($vals[-1]) eq 'HASH');
  };
  if($@ and $@ =~ /.*Duplicate entry/) {
    if($max_retries and $retries < $max_retries) {
      $retries++;
      goto UPDATE_ROW_TOP;
    }
    else {
      die($@);
    }
  }
  if($changed_rows % $c{'batch-size'} == 0) {
    $pl->d("SQL: COMMIT /*", $changed_rows, '/', $c{'batch-size'}, "*/");
    $dbh->commit;
    $dbh->begin_work if($dbh->{AutoCommit});
    if($c{resume}) {
      $resume_info{$cur_tbl} = [$idx_col, $$row{$idx_col}, $min_idx, $max_idx];
      save_resume();
    }
  }
  $changed_rows++;
  if($c{'limit'} and ($changed_rows > $c{'limit'})) {
    die("Reached $c{'limit'} rows");
  }
}

if(!caller) { exit(main(@ARGV)); }

=pod

=head1 NAME

pdb-munch - Flexible data obfuscation tool

=head1 SYNOPSIS

This tool was made to santize records in a table so that
taking it out of the secure environment in which it was created
is feasible. This can be useful for devs who want a copy of "real"
data to take home with them for testing purposes.

This tool is designed to modify your data. B<DO NOT> run it on
production systems. Because it does destructive operations on your
data, it does not accept hostnames on the commandline. Always double
and triple check your configuration before running this tool.

=head1 OPTIONS

=over 8

=item --logfile,-L

Specifies where to log. Can be set to a string like: syslog:<facility> to
do syslog logging. LOCAL0 usually logs to /var/log/messages.

Default: ./pdb-munch.log

=item --dry-run,-n

Only report on what actions would be taken.

=item --dump-spec

Dump the built-in spec file to the file F<default_spec.cnf>.

This is a good starting point for building your own spec file.

=item --spec,-s

Use the column types from this file.

=item --config,-c

Use the host/table configuration in this file.

=item --max-retries

How many times to retry after a unique key error.

Default: 1,000

=item --limit

If used, will stop the tool after --limit rows.

=item --batch-size,-b

Tells pdb-munch to modify --batch-size records at a time.
The batch size also determines the commit interval. For InnoDB,
very long transactions can push other operations out and slow
down the muncher.

Default: 10,000

=item --resume,-r

Load saved resume info from file.
In order to start pdb-munch for a new 

=back

=head1 EXAMPLES

  # Munch the data on test_machine using
  # The default column type specs.
  pdb-munch -d default_spec.conf -c test_machine.conf
  
  # Dumps the built-in spec to default_spec.conf
  # This is a very handy starting point, since the default
  # munch spec includes several datatypes. 
  pdb-munch --dump-spec
  
=head1 CONFIG FILES

The config file defines which host to connect to, and what columns in which
tables to modify. Example:

  [__connection__]
  dsn =   h=testdb,u=root,p=pass,D=testdb
  
  ;; Tables
  ;[table_name]
  ;column_name = type
  
  [addresses]
  address_line_one = address
  name = name
  email = email_righthand
  
The C<__connection__> section has only one parameter: C<dsn>, it specifies
the connection information. It's a list of key-value pairs separated by
commas. Description of keys:

  h - host name
  u - mysql user
  p - mysql password
  D - mysql schema(database)

All are mandatory.
  
=head1 SPEC FILES

Spec files describe different kinds of datatypes stored inside
a mysql column (for instance, an address stored in a varchar column).
They also describe how to modify the data contained in those columns.

A spec file is an Ini style configuration file, composed of one or more
sections following the form:

  [<datatype>]
  column-type = <mysql column type>
  source      = <csv:<file>|list:<comma separated list>|random|module:<file>>
  method      = <random|roundrobin|<perl subroutine name>>
  match<I>    = <perl regex>
  match<I+1>  = <perl regex>
  match<I+N>  = <perl regex>

The spec file should contain the special section C<[__param__]> which controls
the way certain conditions in the muncher are handled. Presently, only one
parameter C<die-on-unmatched> is used, which, if set to a true value will
cause pdb-munch to die if all of the C<< match<I> >> patterns fail. Example:

  [__param__]
  die-on-unmatched = 1

Parameter descriptions:

=over 8

=item C<column-type>

Specifies the type of MySQL column this datatype is stored in.
Presently, this is unused, but required.

=item C<source>

The source is where to pull data from. The most common is from a CSV file.

The C<list:> type is an inline comma separated list of values. It's most
useful for ENUM column types.

C<random> generates several randomly sized random strings per row and selects one.

C<module:> is the most flexible, it allows you to load an arbitrary perl module
and then use the C<method> parameter to call a subroutine in it. The sub will
recieve a handle to the database connection, the column data, the name of the
index column, the name of the current column, and a hashref of the row data.

The perl subroutine is expected to return the new value for the column.
If the subroutine deletes the row in question, it should return a hashref so
that the rest of the main loop is skipped.

The perl subroutine is expected to return the new value for the column.

=item C<method>

One of: random, roundrobin, or the name of a perl subroutine.
For random, the tool will select a random value from the source, for roundrobin,
it'll use them in order as they appear one after another in a loop.
The perl sub, if used, will be called as described above.

=item C<< match<I> >>

For the C<csv:>, C<list:>, and C<random> source types, these define how to
replace the cell contents. For C<csv:> types, each capture group in the regex
corresponds to a column in the CSV. The C<list:> and C<random> types ony support
one capture group.

=back

=head1 ENVIRONMENT

Like all PalominoDB data management tools, this tool responds to the
environment variable C<Pdb_DEBUG>, when it is set, this tool produces
copious amounts of debugging output.

=cut

1;
