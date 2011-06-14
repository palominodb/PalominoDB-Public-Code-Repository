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
# Lockfile package ea5b5b5b18223dde7e589a288174b0376820ef93
# ###########################################################################
package Lockfile;
use strict;
use warnings FATAL => 'all';
use Fcntl qw(:DEFAULT :flock);

sub get {
	my ($class, $file, $timeout) = @_;
	my $self = {};
	my $lock_fh;
	if( not defined $timeout ) {
		$timeout = 0;
	}
	if( not defined $file ) {
		die("Lockfile: Missing mandatory argument: file\n");
	}
	eval {
		my $flocked = 0;
		local $SIG{'ALRM'} = sub { die("timeout.\n"); };
		if( $timeout ) {
			alarm($timeout);
		}
		# Attempt indefinitely (up to alarm time) to get a lock
		# that also has an existing file.
		while(!$flocked) {
			sysopen($lock_fh, $file, O_RDWR | O_CREAT) or die("failed to open $file: $!\n");
			flock($lock_fh, LOCK_EX) or die("failed to flock $file: $!\n");
			if(! -f $file) {
				close($file);
			}
			$flocked = 1;
		}
		alarm(0) if( $timeout );
	};
	if($@ and $@ =~ /timeout/) {
		alarm(0) if( $timeout );
		die("Lockfile: Unable to acquire lock on $file after $timeout seconds");
	}
	elsif($@ and $@ =~ /failed to open/) {
		alarm(0) if( $timeout );
		die("Lockfile: Unable to open lock $file");
	}
	elsif($@ and $@ =~ /failed to flock/) {
		alarm(0) if( $timeout );
		die("Lockfile: Unable to flock $file");
	}
	elsif($@) {
		chomp($_ = "$@");
		alarm(0) if( $timeout );
		die("Lockfile: Unknown error: $_");
	}
	$$self{'file'} = $file;
	$$self{'timeout'} =  $timeout;
	$$self{'fh'} = $lock_fh;
	bless $self, $class;
	return $self;
}

sub DESTROY {
	my ($self) = @_;
	my $fh = $$self{'fh'};

	flock($fh, LOCK_UN) or die("Lockfile: Unable to unlock $$self{'file'}");
	close($fh) or die("Lockfile: Unable to close $$self{'file'}: $!");
	unlink($$self{'file'}) or die("Lockfile: Unable to remove lock $$self{'file'}: $!");
}


1;
# ###########################################################################
# End Lockfile package
# ###########################################################################

# ###########################################################################
# CrashReporter package 20a1dd3b0873e81c50584b381c93fa40cd576a7e
# ###########################################################################
package CrashReporter;
use strict;
use warnings FATAL => 'all';
use IniFile;

use Sys::Hostname;
use File::Spec;
use Time::HiRes qw(time);

my $Mail_Available = 1;
eval 'use Mail::Send';
if($@) {
  $Mail_Available = 0;
}

my @Config_Paths = qw(
	/etc/crash_reporting.cnf
	/usr/local/pdb/etc/crash_reporting.cnf
);
my @App_Handlers;
my $CRASH_CFG_DEFAULTS = {
	'general' => {
		'report-to' => ['brian@palominodb.com'],
		'dump-tracefile' => 'mail-unavailable',
		'tracefile-dir' => '/tmp',
	},
	'report-exclude' => {},
};
my %Crash_Config;
my $Loaded_Config;

sub install {
	my ($class) = @_;
	push @App_Handlers, $SIG{'__DIE__'};
	$SIG{'__DIE__'} = \&_report_crash;

	# Try to load each built-in path.
	foreach my $c_path (@Config_Paths) {
		eval {
			%Crash_Config = IniFile::read_config($c_path);
			ProcessLog::_PdbDEBUG >= ProcessLog::Level3 &&
			$::PL->d('Loaded crash reporting config from:', $c_path);
			$Loaded_Config = $c_path;
		};
		last if(scalar %Crash_Config);
	};
	if(not scalar %Crash_Config) {
		ProcessLog::_PdbDEBUG >= ProcessLog::Level3 &&
		$::PL->d('Loaded crash reporting config from built-in defaults.');
		$Loaded_Config = 'builtin';
	}
	%Crash_Config = %{IniFile::_merge($CRASH_CFG_DEFAULTS, \%Crash_Config)};

	return $App_Handlers[-1];
}

sub add_handler {
	my ($class, $sub) = @_;
	push @App_Handlers, $sub;
}

sub _q {
	return '"'. join('', @_) .'"';
}
sub _qk {
	return '"'. join('', @_) .'": ';
}

sub _report_crash {
	die(@_) if($^S); # ignore exceptions in an eval
	my @args = map { "$_" } @_;
	chomp(@args);
	my @report = ('{');
	my $i = 1;
	my $level = 30;

	push @report, _qk('exception'). _q(@args);
	push @report, _qk('script'). _q($::PL->name()). ',';
	push @report, _qk('pid'). _q($$);
	push @report, _qk('script_args:'). _q(join(' '), map { "[$_]" } @ARGV);
	push @report, _qk('script_run'). _q($::PL->runid()). ',';
	push @report, _qk('script_log'). _q($::PL->{log_path} =~ /^syslog/ ?
                               $::PL->{log_path}
                                 : File::Spec->rel2abs($::PL->{log_path})). ',';

	if(!$Crash_Config{'report-exclude'}{'sys'}) {
		push @report, _qk('hostname') . _q(hostname()). ',';
	}

	if(!$Crash_Config{'report-exclude'}{'env'}) {
		push @report, _qk('environment_vars') . '{';
		foreach my $ek (sort keys %ENV) {
			push @report, _qk($ek). _q($ENV{$ek}). ',';
		}
		$report[-1] =~ s/,$//; # remove trailing comma
		push @report, '},';
	}

	push @report, _qk('stack_trace'). '[';

  my ($package, $file, $line, $sub) = caller($i);
  $i++;
  while($package and $i < $level) {
    push @report, _q("$package,$file,$line,$sub"). ',';
    ($package, $file, $line, $sub) = caller($i);
    $i++;
  }

	$report[-1] =~ s/,$//; # remove trailing comma
	push @report, ']';
	push @report, '}';

	# Send mail, if we've got the ability, and addresses configured.
	if($Mail_Available and scalar @{$Crash_Config{'general'}{'report-to'}}) {
		my $fh;
		my $m = Mail::Send->new(Subject => "Crash Report for ". $::PL->name());
		$m->to(@{$Crash_Config{'general'}{'report-to'}});
		$fh = $m->open();
		print($fh join("\n", @report), "\n");
		$fh->close();
	}

	# Dump a tracefile only when we're configured to always do it,
	# or mail sending is not available.
	# If we're unable to even open a tracefile for writing, then we append
	# some additional details to the original exception and abandon our
	# attempt to write the tracefile.
	if($Crash_Config{'general'}{'dump-tracefile'} eq 'always'
	    or ($Crash_Config{'general'}{'dump-tracefile'} eq 'mail-unavailable'
		 			and !$Mail_Available)) {
		my $report_path = File::Spec->rel2abs(
			$Crash_Config{'general'}{'tracefile-dir'}. '/'.
			'crash-report-'. time(). '.json');
		my $fh;
		if(!open($fh, ">". $report_path)) {
			push @args, ', and additionally while trying to write a crash report to'.
				$report_path;
		}
		else {
			print($fh join("\n", @report), "\n");
		}
	}

	foreach my $hnd (@App_Handlers) {
		next if(not defined $hnd);
		# Call any app handlers - it's up to them to
		# not die, if they don't want to terminate things.
		$hnd->(@args);
	}
	# Finish off by dying in case an app handler did not.
	# This is probably the only reasonable course of action.
	die(@args);
}


1;
# ###########################################################################
# End CrashReporter package
# ###########################################################################

package pdb_tasks_tracker;
use strict;
use warnings FATAL => 'all';
use POSIX ":sys_wait_h";
use DBI;

use Data::Dumper;
use Time::HiRes qw(gettimeofday tv_interval);
use DateTime;
use IO::Dir;
use File::Spec::Functions;

use Getopt::Long qw(:config no_ignore_case);
use Pod::Usage;

my $dsn;
my %o;

sub get_current_timestamp {
  return DateTime->now(time_zone => 'local')->strftime("%Y-%m-%d %H:%M:%S");
}

sub main {
  my @ARGV = @_;
  %o = ('jobs' => 1);
  my (@files, @commands, $sqldir, $prog_lock);
  GetOptions(\%o,
    'help',
    'sqldir=s',
    'condition=s',
    'dsn=s',
    'stats=s',
    'abort-on-error',
    'logfile|L=s',
    'quiet',
    'dryrun|n',
    'lockfile=s',
    'jobs|j=i'
  );

  if($o{'help'}) {
    pod2usage(-verbose => 1);
  }

  if(!$o{'sqldir'}) {
    pod2usage('Error: --sqldir required');
  }
  if(!$o{'dsn'}) {
    pod2usage("Error: --dsn required");
  }
  if(!$o{'stats'}) {
    pod2usage("Error: --stats required");
  }

  $::PL->logpath($o{'logfile'});
  $::PL->quiet($o{'quiet'});
  $dsn = $o{'dsn'};

  eval {
    $dsn = DSNParser->default()->parse($dsn);
    # Do we have mandatory fields? FIXME
  };
  if($@) {
    pod2usage($@);
  }

  if($o{'condition'}) {
    $::PL->i("Waiting for conditional file", $o{'condition'});
    while(! -f $o{'condition'}) {
      sleep(2);
    }
    # We don't need this file anymore. Minimize
    # The chance of another process coming in behind us and duplicating the work.
    unlink($o{'condition'});
    $::PL->i("Remove condition file", $o{'condition'});
  }

  ## Acquire our main program lock, when we leave main()
  ## this will go out of scope and the lock will be released.
  if($o{'lockfile'}) {
    $prog_lock = Lockfile->get($o{'lockfile'});
  }

  ## Fetch our list of SQL files.
  $sqldir = IO::Dir->new($o{'sqldir'}) or die("Couldn't open $o{'sqldir'}: $!\n");
  while(my $filename = $sqldir->read()) {
    next if $filename eq '.' or $filename eq '..';
    $filename = catfile($o{'sqldir'}, $filename);
    if(! -f $filename or ! -r $filename) {
      $::PL->m("Skipping file [$filename]");
      next;
    }
    push(@files, $filename);
  }
  $sqldir->close();

  $::PL->i("Found", scalar(@files), "command files");
  @files = sort @files; # Sort by filename, natural ordering.

  foreach my $filename (@files) {
    my $fh;
    local $/; # set record separator to undef to enable slurp.
    open($fh, "<$filename") || die("Failed to open [$filename]");
    $_ = <$fh>;
    if(/^[\s\n\t]*$/) {
      $::PL->i("Skipping file [$filename]. No content.");
      next;
    }
    push(@commands, {
                      'filename' => $filename,
                      'commands' => $_,
                    }
        );

  }

  ## Test connectivity to target, and stats connections
  ## before attempting to execute any SQL. Early failure is good for you.
  eval {
    my $dbh = $dsn->get_dbh(0);
    $dbh->disconnect();
    my $sdsn = DSNParser->default()->parse($o{'stats'});
    $sdsn->fill_in($dsn);
    $dbh = $sdsn->get_dbh();
  };
  if($@) {
    pod2usage($@);
  }

  $::PL->i("Will write results to ".$o{'stats'});

  my $done = 0;
  my $drain_and_exit = 0;
  my %children;
  $::PL->i("Processing ".scalar(@commands)." commands");
  while(!$done) {
    $::PL->i("In Queue [".scalar(@commands)."] In Flight [".scalar(keys %children)."]");
    if(!$drain_and_exit && scalar(@commands) > 0 && scalar(keys %children) < $o{'jobs'}) {
      # Fork off a new one
      my $pid = fork();
      my $command_data = shift(@commands);
      if($pid) {
        # Parent
        $children{$pid} = 1;
      }
      else {
        # Child
        my $dbh = $dsn->get_dbh(1);
        my $had_error = 0;
        my $i = 0;
        my ($status, $rows, $start, $elapsed, $t0, @warnings)
          = ('', 0, get_current_timestamp(), 0.0, [gettimeofday()], ());

        ## Split queries based on something that Probably will never occur.
        my @cmds = split(/\/\/# ###########################################################################/,
            $command_data->{'commands'}
          );

        foreach my $cmd (@cmds) {
          eval {
            $dbh->{AutoCommit} = 0;
            # Execute the query
            $::PL->i("Executing [$cmd]\n",
                     "from file [".$command_data->{'filename'}."]");
            $rows = $dbh->do($cmd);
            $rows = 0 if $rows eq '0E0';

            my $sth = $dbh->prepare('SHOW WARNINGS');
            $sth->execute();
            while(my $wa = $sth->fetchrow_hashref) {
              push(@warnings, "$wa->{'Message'} ($wa->{'Code'})");
            }

            $dbh->commit();
          };

          $status = "$@";

          $elapsed = tv_interval($t0);
          if($status) {
            $had_error = 1;
            $::PL->e("Command [$cmd]\n",
                     "returned error [".$status."] after",
                     $elapsed, "seconds.");
          }

          # Our results
          save_statistics(
            $command_data->{'filename'} .':'. $i,
            $start,
            $elapsed,
            $rows,
            $status,
            join("\n", @warnings)
          );
          $i++;
        }
        $dbh->disconnect();

        exit($had_error);
      }
    }

    if(scalar(keys %children) > 0) {
      # See if any children have finished.
      my $stiff = waitpid(-1, &WNOHANG);
      if($stiff == 0) {
        # Nothing finished yet, take a break
        sleep(2);
      }
      else {
        my $exit_value = $? >> 8;
        if($exit_value == 0) {
          # Finished without problems
        }
        else {
          if($o{'abort-on-error'}) {
            # Wait for the children in progress and exit without spawning new ones
            $::PL->e("Caught an error. Will wait for in-flight processes to finish and then will abort processing");
            $drain_and_exit = 1;
          }
        }
        $::PL->i("Process [".$stiff."] finished with exit code [".$exit_value."]");
        delete $children{$stiff};
      }
    }
    elsif($drain_and_exit or scalar(@commands) == 0) {
      # We are done
      $done = 1;
    }
  }

  return 0;
}

sub save_statistics {
  my (@args) = @_;
  eval {
    my ($dbh, $tbl);
    my $sdsn = DSNParser->default()->parse($o{'stats'});
    $sdsn->fill_in($dsn);
    $dbh = $sdsn->get_dbh();
    $tbl = $dbh->quote_identifier(undef, $sdsn->get('D'), $sdsn->get('t'));

    $dbh->do(
      qq|INSERT INTO $tbl (`file`, `start_time`, `elapsed`, `rows`, `error`, `warnings`)
          VALUES (?, ?, ?, ?, ?, ?)|,
      undef,
      @args
    );
    $dbh->commit();
    $dbh->disconnect();
  };
  if($@) {
    $::PL->e('Error saving statistics:', "$@");
  }
}

if(!caller) { CrashReporter->install(); exit(main(@ARGV)); }

=pod

=head1 NAME

pdb-tasks-tracker - run tasks and compile information about them

=head1 EXAMPLES

  # Run 4 concurrent jobs out of sql_1/
  pdb-tasks-tracker --sqldir sql_1/ --dsn h=localhost \
                    --stats D=stats,t=tasks --jobs 4

=head1 SYNOPSIS

pdb-tasks-tracker [-h] --sqldir DIR --dsn DSN --stats

Run with -h or --help for options.

=head2 OPTIONS

=over 8

=item --help

This help.

Help is most awesome. It's like soap for your brain.

=item --logfile,-L

Path to a file for logging, or, C<< syslog:<facility> >>
Where C<< <facility> >> is a pre-defined logging facility for this machine.

See also: L<syslog(3)>, L<syslogd(8)>, L<syslog.conf(5)>

=item --quiet

Suppress output to the console.

=item --condition

File to read commands from. This script will sit in a sleep loop until this
file become available. B<NOTE:> This script will B<REMOVE> the condition file
upon picking it up, so, do not set this to a data file.

=item --dsn

DSN of MySQL server to perform commands against.

=item --stats

DSN for where to store SQL run statistics. This need only contain the
D and t keys, since, missing values will be filled in from --dsn.

The table that the results are inserted into must have the form, or a form
compatible with:

  CREATE TABLE `task_stats` (
    `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
    `file` VARCHAR(64) NOT NULL,
    `start_time` DATETIME NOT NULL,
    `elapsed` DECIMAL(10,4) NOT NULL,
    `rows` INTEGER NOT NULL,
    `error` TEXT NOT NULL,
    `warnings` TEXT NOT NULL,
    UNIQUE KEY `file_start` (`file`, `start_time`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

=item --sqldir

Directory of SQL statements to run.
If you need to enforce some ordering, prefix the files with NN_.
This tool will sort the files the way you expect ( 02 < 10 ).

=item --jobs,-j

Number of concurrent SQL queries to run.
The default number of jobs to run is 1, which means: not parallel.

=item --abort-on-error

Processing will immediately stop if an error is encountered.
Normally, this tool will run all available SQL regardless of errors, so
as to collect as much information about the failure as possible (in theory).

=item --lockfile

Acquire flock the given file to ensure that multiple concurrent runs of this
tool do not occur.

=back

=head2 DSN KEYS

All of the standard keys apply. Here a few samples:

  h=localhost,u=root,p=pass,S=/tmp/my.sock,D=db,t=tbl
  h=remotehost,u=bill,p=Zork,N=utf8
  F=params.cnf,h=host

=cut

1;




