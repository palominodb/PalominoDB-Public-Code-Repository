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
# TablePartitions package bf38632f606e6c5d8d6c94691979a33e334690e2
# ###########################################################################
package TablePartitions;
use strict;
use warnings FATAL => 'all';

use English qw(-no_match_vars);
use Data::Dumper;

use DBI;

sub new {
  my ( $class, $pl, $dsn ) = @_;
  my $self = ();
  $self->{dbh} = $dsn->get_dbh(1);
  $self->{pl} = $pl;
  $self->{schema} = $dsn->get('D');
  $self->{name} = $dsn->get('t');
  bless $self, $class;

  $self->_get_partitions();

  if($self->{partition_method} ne 'RANGE') {
    return undef;
  }
  else {
    return $self;
  }
}

sub _get_version {
  my ($self) = @_;
  my $dbh = $self->{dbh};

  my ($version) = $dbh->selectrow_array('SELECT VERSION()');
  my ($major, $minor, $micro, $dist) = $version =~ /^(\d+)\.(\d+)\.(\d+)-(.*)/;
  unless($major) {
    ($major, $minor, $micro) = $version =~ /^(\d+)\.(\d+)\.(\d+)/;
    $dist = '';
  }
  ["$major.$minor", $major, $minor, $micro, $dist];
}

sub _get_partitions {
  my ($self) = @_;
  my $dbh = $self->{dbh};
  my ($release, undef, undef, undef, undef) = $self->_get_version();
  die("Server release not at least 5.1 ($release)") if ($release < 5.1);

  if(1) {
    $self->_get_partitions_by_IS();
  }
}

sub _get_partitions_by_IS {
  my ($self) = @_;
  my $dbh = $self->{dbh};

  my $qtd_schema = $dbh->quote($self->{schema});
  my $qtd_table  = $dbh->quote($self->{name});

  my $sql = "SELECT * FROM `information_schema`.`PARTITIONS` WHERE TABLE_SCHEMA=$qtd_schema AND TABLE_NAME=$qtd_table";

  $self->{pl}->d('SQL:', $sql);

  my $rows = $dbh->selectall_arrayref($sql, { Slice => {} });

  $self->{pl}->es("Table does not have any partitions, or does not exist.")
    and die("Table does not have any partitions, or does not exist")
  unless(scalar @$rows >= 1);

  $self->{partitions} = [];
  $self->{partition_method} = $rows->[0]->{PARTITION_METHOD};
  $self->{partition_expression} = $rows->[0]->{PARTITION_EXPRESSION};
  foreach my $r (@$rows) {
    my $p = {
      name => $r->{PARTITION_NAME},
      sub_name => $r->{SUBPARTITION_NAME},
      position => $r->{PARTITION_ORDINAL_POSITION},
      description => $r->{PARTITION_DESCRIPTION},
      sub_position => $r->{SUBPARTITION_ORDINAL_POSITION}
    };
    push @{$self->{partitions}}, $p;
  }
}

sub partitions {
  my ($self) = @_;
  $self->{pl}->d(Dumper($self->{partitions}));
  $self->{partitions}
}

sub first_partition {
  my ($self) = @_;
  $self->{partitions}->[0];
}

sub last_partition {
  my ($self) = @_;
  $self->{partitions}->[-1];
}

sub method {
  my ($self) = @_;
  $self->{partition_method};
}

sub expression {
  my ($self) = @_;
  $self->{partition_expression};
}

sub expression_column {
  my ($self) = @_;
  my ($col, $fn) = $self->expr_datelike;
  return $col if(defined($col));
  $self->{partition_expression} =~ /^\s*(A-Za-z\-_\$)\(([A-Za-z0-9\-_\$]+)\)/i;
  return $2 if ($1 and $2);
  return $self->{partition_expression};
}

sub expr_datelike {
  my ($self) = @_;
  my %datefuncs = ( 'to_days' => 'from_days', 'month' => 1, 'year' => 1, 'unix_timestamp' => 'from_unixtime' );
  $self->{partition_expression} =~ /^\s*([A-Za-z\-_\$]+)\(([A-Za-z0-9\-_\$]+)\)/i;
  if($datefuncs{lc($1)}) {
    return ($2, $1, $datefuncs{lc($1)});
  }
  else {
    return undef;
  }
}

sub match_partitions {
  my ($self, $reg) = @_;
  my %res;
  map { $res{$_->{name}} = {name => $_->{name}, position => $_->{position}, description => $_->{description} } if($_->{name} =~ $reg); } @{$self->{partitions}};
  values %res;
}

sub has_maxvalue_data {
  my ($self) = @_;
  my $dbh = $self->{dbh};
  my $explain_result = undef;
  my $descr = undef;
  my $col = $self->expression_column;
  if ( $self->{partitions}->[-1]->{description} eq 'MAXVALUE' ) {
    $descr = $self->{partitions}->[-2]->{description};
    if($self->expr_datelike) {
      my (undef, $fn, $cfn) = $self->expr_datelike;
      if($fn) {
        $descr = "$cfn($descr)";
      }
    }
  }
  else {
    return 0; # Can't have maxvalue data since there isn't a partition for that.
  }
  my $sql =
      qq|SELECT COUNT(*) AS cnt
           FROM `$self->{schema}`.`$self->{name}`
         WHERE $col > $descr
        | ;
  $self->{pl}->d('SQL:', $sql);
  eval {
    $explain_result = $dbh->selectrow_hashref($sql);
    $self->{pl}->d(Dumper($explain_result));
  };
  if($EVAL_ERROR) {
    $self->{pl}->es($EVAL_ERROR);
    return undef;
  }
  return $explain_result->{cnt};
}

sub start_reorganization {
  my ($self, $p) = @_;
  die("Need partition name to re-organize") unless($p);
  my $part = undef;
  foreach my $par (@{$self->{partitions}}) {
    $part = $par if($par->{name} eq $p);
  }
  return undef unless($part);
  $self->{re_organizing} =  [];
  push @{$self->{re_organizing}},$part;
  return 1;
}

sub add_reorganized_part {
  my ($self, $name, $desc) = @_;
  return undef unless($self->{re_organizing});
  my ($col, $fn) = $self->expr_datelike;
  push @{$self->{re_organizing}}, {name => $name, description => $desc};
  return 1;
}

sub end_reorganization {
  my ($self, $pretend) = @_;
  return undef unless $self->{re_organizing};
  my $sql = "ALTER TABLE `$self->{schema}`.`$self->{name}` REORGANIZE PARTITION";
  my $orig_part = shift @{$self->{re_organizing}};
  my (undef, $fn) = $self->expr_datelike;
  $sql .= " $orig_part->{name} INTO (";
  while($_ = shift @{$self->{re_organizing}}) {
      $sql .= "\nPARTITION $_->{name} VALUES LESS THAN ";
    if(uc($_->{description}) eq 'MAXVALUE') {
      $sql .= 'MAXVALUE';
    }
    else {
      if($fn) {
        $sql .= "($fn(" . $self->{dbh}->quote($_->{description}) . '))';
      }
      else {
        $sql .= "(" . $_->{description} . ')';
      }
    }
    $sql .= ',';
  }
  chop($sql);
  $sql .= "\n)";
  $self->{pl}->d("SQL: $sql");
  eval {
    unless($pretend) {
      $self->{dbh}->do($sql);
      $self->_get_partitions();
    }
  };
  if($EVAL_ERROR) {
    $self->{pl}->e("Error reorganizing partition $orig_part->{name}: $@");
    return undef;
  }
  $self->{re_organizing} = 0;
  return 1;
}

sub add_range_partition {
  my ($self, $name, $description, $pretend) = @_;
  if($self->method ne 'RANGE') {
    $self->{pl}->m("Unable to add partition to non-RANGE partition scheme.");
    return undef;
  }
  for my $p (@{$self->{partitions}}) {
    if($p->{description} eq 'MAXVALUE') {
      $self->{pl}->m("Unable to add new partition when a catchall partition ($p->{name}) exists.");
      return undef;
    }
  }
  my (undef, $fn, $cfn) = $self->expr_datelike;
  my $qtd_desc = $self->{dbh}->quote($description);
  $self->{pl}->d("SQL: ALTER TABLE `$self->{schema}`.`$self->{name}` ADD PARTITION (PARTITION $name VALUES LESS THAN ($fn($qtd_desc)))");
  eval {
    unless($pretend) {
      $self->{dbh}->do("ALTER TABLE `$self->{schema}`.`$self->{name}` ADD PARTITION (PARTITION $name VALUES LESS THAN ($fn($qtd_desc)))");
      $self->_add_part($name, "to_days($qtd_desc)");
    }
  };
  if($EVAL_ERROR) {
    $self->{pl}->e("Error adding partition: $@");
    return undef;
  }
  return 1;
}

sub drop_partition {
  my ($self, $name, $pretend) = @_;
  if($self->method ne 'RANGE') {
    $self->{pl}->m("Unable to drop partition from non-RANGE partition scheme.");
    return undef;
  }
  $self->{pl}->d("SQL: ALTER TABLE `$self->{schema}`.`$self->{name}` DROP PARTITION $name");
  eval {
    unless($pretend) {
      $self->{dbh}->do("ALTER TABLE `$self->{schema}`.`$self->{name}` DROP PARTITION $name");
      $self->_del_part($name);
    }
  };
  if($EVAL_ERROR) {
    $self->{pl}->e("Error dropping partition: $@");
    return undef;
  }

  return 1;
}

sub desc_from_datelike {
  my ($self, $name) = @_;
  my ($desc, $fn, $cfn) = $self->expr_datelike;

  if($self->method ne 'RANGE') {
    $self->{pl}->d("Only makes sense for RANGE partitioning.");
    return undef;
  }
  return undef if(!$fn);

  for my $p (@{$self->{partitions}}) {
    if($p->{name} eq $name) {
      $desc = $p->{description};
      last;
    }
  }

  $self->{pl}->d("SQL: SELECT $cfn($desc)");
  my ($ds) = $self->{dbh}->selectrow_array("SELECT $cfn($desc)");
  return $ds;
}

sub _add_part {
  my ($self, $name, $desc) = @_;
  my ($d) = $self->{dbh}->selectrow_array("SELECT $desc");
  push @{$self->{partitions}}, {name => $name, description => $d, position => undef};
}

sub _del_part {
  my ($self, $name) = @_;
  my @replace = ();
  foreach my $p (@{$self->{partitions}}) {
    unless($p->{name} eq $name) {
      push @replace, $p;
    }
  }
  $self->{partitions} = \@replace;
}

1;

# ###########################################################################
# End TablePartitions package
# ###########################################################################

# ###########################################################################
# Timespec package 9c2ee59ea0b33f8cb8791bf3336cea9bc52d8643
# ###########################################################################
package Timespec;
use strict;
use warnings FATAL => 'all';
use DateTime;
use DateTime::Format::Strptime;
use Carp;

sub parse {
  my ($class, $str, $ref) = @_;
  if(not defined $ref) {
    $ref = DateTime->now(time_zone => 'local');
  }
  else {
    $ref = $ref->clone();
  }
  my $fmt_local = DateTime::Format::Strptime->new(pattern => '%F %T',
                                                  time_zone => 'local');
  my $fmt_tz = DateTime::Format::Strptime->new(pattern => '%F %T %O');
  $fmt_tz->parse_datetime($str);
  if($str =~ /^([-+]?)(\d+)([hdwmqy])(?:(?:\s|\.)(startof))?$/) {
    my ($spec, $amt) = ($3, $2);
    my %cv = ( 'h' => 'hours', 'd' => 'days', 'w' => 'weeks', 'm' => 'months', 'y' => 'years' );
    if($4) {
      if($cv{$spec}) {
        $_ = $cv{$spec};
        s/s$//;
        $ref->truncate(to => $_);
      }
      else { # quarters
        $ref->truncate(to => 'day');
        $ref->subtract(days => $ref->day_of_quarter()-1);
      }
    }

    if($spec eq 'q') {
      $spec = 'm';
      $amt *= 3;
    }

    if($1 eq '-') {
      $ref->subtract($cv{$spec} => $amt);
    }
    if($1 eq '+' or $1 eq '') {
      $ref->add($cv{$spec} => $amt);
    }
    return $ref;
  }
  elsif($str eq 'now') {
    return DateTime->now(time_zone => 'local');
  }
  elsif($str =~ /^(\d+)$/) {
    return DateTime->from_epoch(epoch => $1);
  }
  elsif($_ = $fmt_tz->parse_datetime($str)) {
    return $_;
  }
  elsif($_ = $fmt_local->parse_datetime($str)) {
    return $_;
  }
  else {
    croak("Unknown or invalid Timespec [$str] supplied.");
  }
}


1;
# ###########################################################################
# End Timespec package
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

package pdb_parted;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);



use DBI;
use Getopt::Long qw(:config no_ignore_case pass_through);
use Pod::Usage;
use DateTime;
use DateTime::Duration;
use DateTime::Format::Strptime;

use Data::Dumper;
{
  no warnings 'once';
  $Data::Dumper::Indent = 0;
  $Data::Dumper::Sortkeys = 1;
}

my $PL = $::PL ? $::PL : ProcessLog->new($0, '/dev/null');

my %o = (
  prefix  => 'p',
  logfile => '/dev/null',
);

sub main {
  @ARGV = @_;
  my (
    $r,
    $dsn,
    $remote_dsn,
    $parts,
    $timespec,
    $requested_dt,
    $email_log,
    @partitions
  );

  GetOptions(\%o,
    "help|h",
    "dryrun|n",
    "logfile|L=s",
    "quiet|q",
    "email-to|E=s",
    "email-activity",
    "prefix|P=s",
    "interval|i=s",
    "limit=i",
    "add",
    "drop",
    "archive",
    "archive-path=s",
    "archive-database=s",
    "i-am-sure",
  );

  $timespec = shift @ARGV;
  $dsn    = shift @ARGV;
  pod2usage("Missing TIMESPEC") if(!$timespec);
  pod2usage("Missing DSN") if(!$dsn);

  $email_log    = '';
  eval {
    $requested_dt = Timespec->parse($timespec);
    $dsn          = DSNParser->default()->parse($dsn);
  };
  if($@) {
    pod2usage($@);
  }

  unless($o{'drop'}) {
    # interval is not necessary for --drop.
    unless($o{'interval'} and $o{'interval'} =~ /^[hdwmqy]$/) {
      pod2usage("interval must be one of: h,d,w,m,q,y");
    }
  }

  unless($o{'prefix'} =~ /^[A-Za-z][A-Za-z0-9_-]*$/) {
    pod2usage("--prefix ($o{'prefix'}) must not include non alpha-numeric characters.");
  }

  unless($o{'add'} or $o{'drop'}) {
    pod2usage("ACTION required");
  }

  if($o{'add'} and $o{'drop'}) {
    pod2usage("Cannot perform more than one action at once");
  }

  if($o{'email-activity'} and !$o{'email-to'}) {
    pod2usage("--email-activity can only be used with --email-to.");
  }

  if($o{'archive-database'}) {
    eval {
      $remote_dsn = DSNParser->default()->parse($o{'archive-database'});
      $remote_dsn->mand_key('D');
      $remote_dsn->mand_key('t');
    };
    if($@) {
        pod2usage($@);
    }
  }

  $PL->start();
  $PL->email_to($o{'email-to'});
  $parts = TablePartitions->new($PL, $dsn);

  if($o{'add'}) {
    $email_log = "Adding partitions to ". $dsn->get('h') .
      "." . $dsn->get('D') . "." . $dsn->get('t') . ":\n";
    my $last_p = $parts->last_partition;
    my $last_d = to_date($parts->desc_from_datelike($last_p->{name}));
    if($last_d >= $requested_dt) {
      $PL->m("At least the requested partitions exist already.\n",
             'Requested out to:', $requested_dt->ymd(), "\n",
             'Partitions out to:', $last_d->ymd(), 'exist.');
      $r = 0; # success
      goto DONE;
    }
    eval {
      @partitions = add_partitions($dsn, $parts, $requested_dt, %o);
      $r = 0;
    };
    if($@) {
      $_ = "$@";
      $PL->e("Error adding partitions:", $_);
      $r = 1;
      goto DONE;
    }

    if($o{'email-activity'}) {
      for(@partitions) {
        $email_log .= "- $_->{name} [older than: $_->{date}]\n";
      }
      $PL->send_email("Partitions added on ". $dsn->get('h') .
                      "." . $dsn->get('D') . "." . $dsn->get('t'), $email_log);
    }
  }
  elsif($o{'drop'}) {
    $email_log = "Dropped partitions from ". $dsn->get('h') .
      "." . $dsn->get('D') . "." . $dsn->get('t') . ":\n";

    eval {
      @partitions = drop_partitions($dsn, $remote_dsn, $parts, $requested_dt, %o);
      $r = 0;
    };
    if($@) {
      $_ = "$@";
      $PL->e("Error dropping partitions:", $_);
      $r = 1;
      goto DONE;
    }

    if($o{'email-activity'}) {
      for(@partitions) {
        $email_log .= "- $_->{name} [older than: $_->{date}]\n";
      }
      $PL->send_email("Partitions dropped on ". $dsn->get('h') .
                      "." . $dsn->get('D') . "." . $dsn->get('t'), $email_log);
    }
  }

  DONE:
  $PL->failure_email() if($r);
  $PL->end();
  return $r;
}

sub interval {
  my $interval = shift;
  my %i = ( 'h' => 'hours', 'd' => 'days', 'w' => 'weeks',
            'm' => 'months', 'y' => 'years' );
  if($interval eq 'q') {
    return DateTime::Duration->new( months => 3 );
  }
  return DateTime::Duration->new( $i{$interval} => 1 );
}

sub add_partitions {
  my ($dsn, $parts, $end_date, %o) = @_;
  my $dbh = $dsn->get_dbh(1);
  die("missing mandatory argument prefix\n") unless($o{'prefix'});
  my $db_host = $dsn->get('h');
  my $db_schema = $dsn->get('D');
  my $db_table = $dsn->get('t');
  my $prefix = $o{'prefix'};
  my $i_am_sure = $o{'i-am-sure'};
  my $dryrun = $o{'dryrun'};
  my $interval = interval($o{'interval'});
  my @parts = ();

  my $i = 0;
  my $ret = 0;
  my $last_p = $parts->last_partition;
  my $next_pN = undef;
  my $curs_date = undef;

  my $reorganize = uc($last_p->{description}) eq 'MAXVALUE';

  if ($reorganize) {
    $last_p = $parts->partitions()->[-2];
    if ($parts->has_maxvalue_data and !$i_am_sure) {
      die("Data in MAXVALUE partition exists.\n");
    }
  }

  $last_p->{name} =~ /^$prefix(\d+)$/;
  $next_pN = $1;
  die("most recent partition didn't match /^$prefix(\\d+)\$/.\n")
    if (not defined($next_pN));
  $next_pN++;

  $last_p->{date} = to_date($parts->desc_from_datelike($last_p->{name}));
  $curs_date = $last_p->{date};

  $PL->d('Last partition:', $last_p->{date}->ymd);
  $PL->d('End date:', $end_date->ymd);

  ###########################################################################
  # Just loop until $curs_date (date cursor) is greater than
  # where we want to be. We advance the cursor by $range increments.
  ###########################################################################
  $i = 0;
  while ($curs_date < $end_date) {
    last if($o{'limit'} and $i >= $o{'limit'});
    push(@parts, {
      name => "$prefix". ($next_pN+$i),
      date => $curs_date->add_duration($interval)->clone(),
    });
    $i++;
  }

  $PL->i('Will add', scalar @parts, 'partition(s).', "\n",
         "Partitions: ",
         join(', ', map { "$_->{name}($_->{date})" } @parts), "\n");

  if ($reorganize) {
    $parts->start_reorganization($parts->last_partition()->{name});
    push(@parts, { name => "$prefix". ($next_pN+$i), date => 'MAXVALUE' });
  }

  ###########################################################################
  # Loop over the calculated dates and add partitions for each one
  ###########################################################################
  foreach my $part (@parts) {
    my $name = $part->{name};
    my $date = $part->{date};
    if ($reorganize) {
      if ($date eq 'MAXVALUE') {
        $parts->add_reorganized_part($part->{name}, $date);
      } else {
        $parts->add_reorganized_part($part->{name}, $date->ymd);
      }
    } else {
      $ret = $parts->add_range_partition($part->{name}, $date->ymd, $dryrun);
      if(!$ret) {
        die("$part->{name} $part->{date}\n");
      }
    }
  }

  if ($reorganize) {
    $ret = $parts->end_reorganization($dryrun);
    if(!$ret) {
      die("re-organizing\n");
    }
  }

  return @parts;
}

sub drop_partitions {
  my ($dsn, $remote_dsn, $parts, $requested_dt, %o) = @_;
  my @drops;
  foreach my $part (@{$parts->partitions()}) {
    $part->{date} = to_date($parts->desc_from_datelike($part->{name}));
    if($part->{date} < $requested_dt) {
      push @drops, $part;
    }
    last if($o{'limit'} and scalar @drops >= $o{'limit'});
  }

  $PL->i('Will drop', scalar @drops, 'partition(s).', "\n",
         "Partitions: ",
         join(', ', map { "$_->{name}($_->{date})" } @drops), "\n");

  foreach my $part (@drops) {
    if($o{'archive'}) {
      archive_partition($dsn, $remote_dsn, $parts, $part, %o);
    }
    if(!$parts->drop_partition($part->{name}, $o{'dryrun'})) {
      die("$part->{name} $part->{date}");
    }
  }
  return @drops;
}

sub archive_partition {
  my ($dsn, $remote_dsn, $parts, $part, %o) = @_;
  my $path = $o{'archive-path'} || "";
  if($path) {
    $path =~ s/[^\/]$/\//;
  }
  my $host = $dsn->get('h');
  my $user = $dsn->get('u');
  my $pw = $dsn->get('p');
  my $schema = $dsn->get('D');
  my $table = $dsn->get('t');

  my $dfile = $dsn->get('F');
  my $r;

  my ($remote_host, $remote_user, $remote_pw, $remote_schema, $remote_table);
  if($remote_dsn) {
    $remote_host = $remote_dsn->get('h');
    $remote_user = $remote_dsn->get('u');
    $remote_pw = $remote_dsn->get('p');
    $remote_schema = $remote_dsn->get('D');
    $remote_table = $remote_dsn->get('t');
  }

  my $create_file = "${path}$host.$schema.$table.". $part->{name} . ".CREATE.sql";
  my $create_clean_file = "${path}$host.$schema.$table.". $part->{name} . ".CREATE.CLEAN.sql";

  my ($desc, $fn, $cfn) = $parts->expr_datelike();
  if($cfn) {
    $desc = "$cfn(". $part->{description} . ")";
  }
  else {
    $desc = $part->{description};
  }
  my @dump_EXEC;

  # Archive to file
  my $output_file = "${path}$host.$schema.$table.". $part->{name} . ".sql";
  @dump_EXEC = ("mysqldump",
                ( $dfile ? ("--defaults-file=$dfile") : () ),
                "--no-create-info",
                "--result-file=". $output_file,
                ($host ? ("-h$host") : () ),
                ($user ? ("-u$user") : () ),
                ($pw ? ("-p$pw") : () ),
                "-w ". $parts->expression_column() . "<$desc",
                $schema,
                $table);
  $PL->i("Archiving:", $part->{name}, "to", $output_file);

  $PL->d("Executing:", @dump_EXEC);
  unless($o{'dryrun'}) {
    $r = $PL->x(sub { system(@_) }, @dump_EXEC);
  }
  else {
    $r = { rcode => 0, error => '', fh => undef };
  }
  if(($$r{rcode} >> 8) != 0) {
    $_ = $$r{fh};
    while (<$_>) { $PL->e($_); }
    $PL->e("got:", ($$r{rcode} >> 8), "from mysqldump.");
    die("archiving $host.$schema.$table.$part->{name}\n");
  }

  if($remote_dsn) {
    # Archive to another database

    # Invoke the commands on the remote database and archive our data.
    # This assumes that the remote database and table have already been created.
    $PL->i("Archiving:", $part->{name}, "to $remote_schema on $remote_host");
    my @archive_EXEC = ("mysql",
                        ( $dfile ? ("--defaults-file=$dfile") : () ),
                        ($remote_host ? ("-h$remote_host") : () ),
                        ($remote_user ? ("-u$remote_user") : () ),
                        ($remote_pw ? ("-p$remote_pw") : () ),
                        '--execute=source '.$output_file,
                        $remote_schema,
        );

    unless($o{'dryrun'}) {
      $r = $PL->x(sub { system(@_) }, @archive_EXEC);
    }
    else {
      $r = { rcode => 0, error => '', fh => undef };
    }
    if(($$r{rcode} >> 8) != 0) {
      $_ = $$r{fh};
      while (<$_>) { $PL->e($_); }
      $PL->e("got:", ($$r{rcode} >> 8), "from mysqldump.");
      die("archiving $host.$schema.$table.$part->{name}\n");
    }
  }
}

sub to_date {
  my ($dstr) = @_;
  #############################################################################
  # MySQL can return two different kinds of dates to us.
  # For DATE columns we just get the date. Obviously.
  # For virtually all other time related columns, we also get a time.
  # This method first tries parsing with just dates and then tries with time.
  #############################################################################
  my $fmt1 = DateTime::Format::Strptime->new(pattern => '%Y-%m-%d', time_zone => 'local');
  my $fmt2 = DateTime::Format::Strptime->new(pattern => '%Y-%m-%d %T', time_zone => 'local');
  return ($fmt1->parse_datetime($dstr) || $fmt2->parse_datetime($dstr))->truncate( to => 'day' );
}


if(!caller) { exit(main(@ARGV)); }

=pod

=head1 NAME

pdb-parted - MySQL partition management script

=head1 EXAMPLES

  # Create weekly partitions for the next quarter to test.part_table
  pdb-parted --add --interval w +1q h=localhost,D=test,t=part_table

  # Create daily partitions for the next 2 weeks
  # starting exactly at the beginning of every day
  pdb-parted --add --interval d +2w.startof h=localhost,D=test,t=part_table

  # Email ops@example.com about partitions added
  pdb-parted --add --email-activity --email-to ops@example.com \
             --interval d +4w h=localhost,D=test,t=part_table

  # Drop partitions older than 8 weeks
  pdb-parted --drop -8w h=localhost,D=test,t=part_table

  # Drop partitions older than Dec 20th, 2010, but only 5 of them.
  pdb-parted --drop --limit 5 '2010-12-20 00:00:00' \
             h=localhost,D=test,t=part_table

  # Drop and archive partitions older than 2 quarters ago.
  pdb-parted --drop --archive --archive-path /backups -2q \
             h=locahost,D=test,t=part_table

  # Same as above, but archived to a separate database.
  pdb-parted --drop --archive --archive-database h=remotehost,D=test_archives,t=part_table -2q \
             h=locahost,D=test,t=part_table

  # Logging to syslog
  pdb-parted --logfile syslog:LOCAL0 --add --interval d 1y \
             h=localhost,D=test,t=part_table


=head1 SYNOPSIS

pdb-parted [options] ACTION TIMESPEC DSN

This tool assists in the creation of partitions in regular intervals.
It creates partitions in regular intervals up to some maximum future date.

  --help,          -h   This help. See C<perldoc pdb-parted> for full docs.
  --dryrun,        -n   Report on actions without taking them.
  --logfile,       -L   Direct output to given logfile. Default: none.

  --email-activity      Send a brief email report of actions taken.
                        The email is sent to --email-to.
  --email-to,      -E   Where to send activity and failure emails.
                        Default: none.

  --prefix,        -P   Partition prefix. Defaults to 'p'.

  --archive             Archive partitions before dropping them.
  --archive-path        Directory to place mysqldumps.
                        Default: current directory.
  --archive-database    Database to archive partitions to.
                        Default: none

  --limit,         -m   Limit the number of actions to be performed.
                        Default: 0 (unlimited)

=head2 ACTION

  --add   Add partitions.
  --drop  Remove partitions.

=head2 TIMESPEC

A timespec is a "natural" string to specify how far in advance to create
partitions. A sampling of possible timespecs:

  1w (create partitions one week in advance)
  1m (one month)
  2q (two quarters)
  5h (five hours)

See the full documentation for a complete description of timespecs.

=head2 DSN

DSNs, such as those passed as option values, or arguments to a program
are of the format: C<({key}={value}(,{key}={value})*>. That is, a C<key=value> pair, followed
by a comma, followed by any number of additional C<key=value> pairs separated by
commas.

Examples:

  h=testdb1,u=pdb,p=frogs
  h=localhost,S=/tmp/mysql.sock,u=root,F=/root/my.cnf

Where 'h' is a hostname, 'S' is a socket path, 'u' is a user, 'F' is a path
to a defaults file, and 'p' is a password. These are non-exhaustive examples.

=head1 TIMESPEC

A timespec is one of:

  A modifier to current local time,
  A unix timestamp (assumed in UTC),
  The string 'now' to refer to current local time,
  An absolute time in 'YYYY-MM-DD HH:MM:SS' format,
  An absolute time in 'YYYY-MD-DD HH:MM:SS TIMEZONE' format.

For the purposes of this module, TIMEZONE refers to zone names
created and maintained by the zoneinfo database.
See L<http://en.wikipedia.org/wiki/Tz_database> for more information.
Commonly used zone names are: Etc/UTC, US/Pacific and US/Eastern.

Since the last four aren't very complicated, this section describes
what the modifiers are.

A modifer is, an optional plus or minus sign followed by a number,
and then one of:

  y = year, q = quarter , m = month, w = week, d = day, h = hour

Followed optionally by a space or a period and 'startof'.
Which is described in the next section.

Some examples (the time is assumed to be 00:00:00):

  -1y         (2010-11-01 -> 2009-11-01)
   5d         (2010-12-10 -> 2010-12-15)
  -1w         (2010-12-13 -> 2010-12-07)
  -1q startof (2010-05-01 -> 2010-01-01)
   1q.startof (2010-05-01 -> 2010-07-01)

=head2 startof

The 'startof' modifier for timespecs is a little confusing,
but, is the only sane way to achieve latching like behavior.
It adjusts the reference time so that it starts at the beginning
of the requested type of interval. So, if you specify C<-1h startof>,
and the current time is: C<2010-12-03 04:33:56>, first the calculation
throws away C<33:56> to get: C<2010-12-03 04:00:00>, and then subtracts
one hour to yield: C<2010-12-03 03:00:00>.

Diagram of the 'startof' operator for timespec C<-1q startof>,
given the date C<2010-05-01 00:00>.

          R P   C
          v v   v
   ---.---.---.---.---.--- Dec 2010
   ^   ^   ^   ^   ^   ^
   Jul Oct Jan Apr Jul Oct
  2009    2010

  . = quarter separator
  C = current quarter
  P = previous quarter
  R = Resultant time (2010-01-01 00:00:00)

=head1 OPTIONS

=over 8

=item --help, -h

This help.

=item --dryrun, -n

Report on actions that would be taken. Works best with the C<Pdb_DEBUG> environment variable set to true.

See also: L<ENVIRONMENT>

=item --logfile, -L

Path to a file for logging, or, C<< syslog:<facility> >>
Where C<< <facility> >> is a pre-defined logging facility for this machine.

See also: L<syslog(3)>, L<syslogd(8)>, L<syslog.conf(5)>

=item --email-to, -E

Where to send emails.

This tool can send emails on failure, and whenever it adds, drops, or archive partitions.
Ordinarily, it will only send emails on failure.

=item --email-activity

If this flag is present, then this will make the tool also email
whenver it adds, drops, or archives a partition.

=item --prefix, -P

Prefix for partition names. Partitions are always named like: <prefix>N.
Where N is a number. Default is 'p', which was observed to be the most common prefix.

=item --interval, -i

type: string one of: d w m y

Specifies the size of the each partition for the --add action.
'd' is day, 'w' is week, 'm' is month, and 'y' is year.

=item --limit

Specifies a limit to the number of partitions to add, drop, or archive.
By default this is unlimited (0), so, for testing one usually wishes to set
this to 1.

=item --archive

type: boolean

mysqldump partitions to files B<in the current directory> named like <host>.<schema>.<table>.<partition_name>.sql

There is not currently a way to archive without dropping a partition.

=item --archive-path

What directory to place the SQL dumps of partition data in.

=item --archive-database

What database to place the archived partitions in.

=back

=head1 ACTIONS

=over 8

=item --add

Adds partitions till there are at least TIMESPEC L<--interval> sized future buckets.

The adding of partitions is not done blindly. This will only add new partitions
if there are fewer than TIMESPEC future partitions. For example:

  Given: --interval d, today is: 2011-01-15, TIMESPEC is: +1w,
         last partition (p5) is for 2011-01-16;

  Result:
    Parted will add 6 partitions to make the last partition 2011-01-22 (p11).

  Before:
   |---+|
  p0  p5

  After:
   |---+-----|
  p0  p5    p11

You can think of C<--add> as specifying a required minimum safety zone.

=item --drop

Drops partitions strictly older than TIMESPEC.
The partitions are not renumbered to start with p0 again.

  Given: today is: 2011-01-15, TIMESPEC is: -1w,
         first partition (p0) is for 2011-01-06


  Result: 2 partitions will be dropped.

  Before: |-----+--|
          0     6  9
  After : |---+--|
          2   6  9

=back

=head1 HISTORY

Previous versions of this tool took complicated and error-prone steps
to normalize the ending date to be exactly on the requested date. This
would result in oddly sized partitions being added if the tool wasn't
run on the same day of the week or month.

This version no longer performs those steps and instead adds exactly
sized partitions starting from the last partition on a table until
there are partitions to cover at least the requested end date. If the
partitions run over that date, it's considered unimportant.

=head1 ENVIRONMENT

Almost all of the PDB (PalominoDB) tools created respond to the environment variable C<Pdb_DEBUG>.
This variable, when set to true, enables additional (very verbose) output from the tools.

=cut

1;
