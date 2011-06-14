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
use warnings;
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
# ZRMBackup package eff67837be96b6ab0faba30b78749ada27ec12db
# ###########################################################################
package ZRMBackup;
use strict;
use warnings FATAL => 'all';

use English qw(-no_match_vars);
use Data::Dumper;
use File::Spec;


sub new {
  my ( $class, $pl, $backup_dir ) = @_;
  my $self = {};
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

# ###########################################################################
# MysqlBinlogParser package 32f75331689a9f1e63dd16a6972c8901e93696ef
# ###########################################################################

package MysqlBinlogParser;
use strict;
use warnings FATAL => 'all';
use MIME::Base64;
use Fcntl qw(:seek);
use Carp;

use constant {
  MAGIC_LEN => 4,
  MAGIC_BYTES => "\xfe\x62\x69\x6e",
  V1_HEADER_LEN => 13,
  V3_HEADER_LEN => 19,
  V4_HEADER_LEN => 19,
  SERVER_VERSION_LEN => 50,

  V1_EVENT_START_LEN => 69,
  V3_EVENT_START_LEN => 75,
  EVENT_FORMAT_DESC_LEN => 91
};

use constant {
  LOG_EVENT_BINLOG_IN_USE_F => 0x01,
  LOG_EVENT_THREAD_SPECIFIC_F => 0x04,
  LOG_EVENT_SUPPRESS_USE_F => 0x08,
  LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F => 0x10,
  LOG_EVENT_ARTIFICIAL_F => 0x20,
  LOG_EVENT_RELAY_LOG_F => 0x40
};

use constant {
  EVENT_UNKNOWN=> 0,
  EVENT_START_V3=> 1,
  EVENT_QUERY=> 2,
  EVENT_STOP=> 3,
  EVENT_ROTATE=> 4,
  EVENT_INTVAR=> 5,
  EVENT_LOAD=> 6,
  EVENT_SLAVE=> 7,
  EVENT_CREATE_FILE=> 8,
  EVENT_APPEND_BLOCK=> 9,
  EVENT_EXEC_LOAD=> 10,
  EVENT_DELETE_FILE=> 11,
  EVENT_NEW_LOAD=> 12,
  EVENT_RAND=> 13,
  EVENT_USER_VAR=> 14,
  EVENT_FORMAT_DESCRIPTION=> 15,
  EVENT_XID=> 16,
  EVENT_BEGIN_LOAD_QUERY=> 17,
  EVENT_EXECUTE_LOAD_QUERY=> 18,
  EVENT_TABLE_MAP => 19,
  EVENT_PRE_GA_WRITE_ROWS => 20,
  EVENT_PRE_GA_UPDATE_ROWS => 21,
  EVENT_PRE_GA_DELETE_ROWS => 22,
  EVENT_WRITE_ROWS => 23,
  EVENT_UPDATE_ROWS => 24,
  EVENT_DELETE_ROWS => 25,
  EVENT_INCIDENT=> 26,
};

use constant {
  Q_FLAGS2_CODE => 0,
  Q_SQL_MODE_CODE => 1,
  Q_CATALOG_CODE => 2,
  Q_AUTO_INCREMENT => 3,
  Q_CHARSET_CODE => 4,
  Q_TIME_ZONE_CODE => 5,
  Q_CATALOG_NZ_CODE => 6,
  Q_LC_TIME_NAMES_CODE => 7,
  Q_CHARSET_DATABASE_CODE => 8,
  Q_TABLE_MAP_FOR_UPDATE_CODE => 9
};

use constant {
  U_STRING_RESULT => 0,
  U_REAL_RESULT   => 1,
  U_INT_RESULT    => 2,
  U_ROW_RESULT    => 4,
  U_DECIMAL_RESULT => 5
};

sub new {
  my $class = shift;
  return $class->open(@_);
}

sub open {
  my ($class, $path_or_fh) = @_;
  my $self = {};
  bless $self, $class;
  if(ref($path_or_fh) and ref($path_or_fh) eq 'GLOB') {
    $$self{fh} = $path_or_fh;
    $$self{path} = '';
  }
  else {
    my $tmpfh;
    open($tmpfh, "<", $path_or_fh) or croak($!);
    binmode($tmpfh);
    $$self{fh} = $tmpfh;
    $$self{path} = $path_or_fh;
  }

  $$self{header_length} = 0;
  $$self{log_version} = -1;
  $$self{closed_properly} = -1;
  $$self{created_at} = -1;
  $self->_read_header;
  return $self;
}

sub _new_event {
  my ($evt_time, $evt_type, $evt_len, $srv_id, $next, $flags) = @_;
  return { ts => $evt_time, type => $evt_type,
           len => $evt_len, server_id => $srv_id,
           next_position => $next, flags => $flags
         };
}

sub _read_header {
  my ($self) = @_;
  my $fh = $$self{fh};
  my ($buf, $evt_type, $srv_id, $evt_len);
  my $header_event;
  CORE::read($fh, $buf, MAGIC_LEN) or croak($!);
  croak("Invalid binlog magic '$buf'") unless($buf eq MAGIC_BYTES);
  CORE::read($fh, $buf, V1_HEADER_LEN) or croak($!);
  ($$self{created_at}, $evt_type, $srv_id, $evt_len) = unpack('LCLL', $buf);
  $header_event = _new_event($$self{created_at}, $evt_type, $evt_len, $srv_id);


  if($evt_type == EVENT_START_V3 and $evt_len == V1_EVENT_START_LEN) {
    $$self{log_version} = 1;
    croak('Binlogs in v1 format not supported');
  }
  elsif($evt_type == EVENT_START_V3 and $evt_len == V3_EVENT_START_LEN) {
    $$self{log_version} = 3;
    croak('Binlogs in v3 format not supported');
  }
  elsif($evt_type == EVENT_FORMAT_DESCRIPTION) {
    $$self{log_version} = 4;
    $self->_format_description_event($header_event);
    $$self{header_length} = V4_HEADER_LEN;
    $$self{header} = $header_event;

    $$self{handlers} = [];
    $$self{handlers}->[EVENT_QUERY] = \&_v4_query_event;
    $$self{handlers}->[EVENT_XID] = \&_v4_xid_event;
    $$self{handlers}->[EVENT_BEGIN_LOAD_QUERY] = \&_v4_append_block_event;
    $$self{handlers}->[EVENT_EXECUTE_LOAD_QUERY] = \&_v4_execute_load_query_event;
    $$self{handlers}->[EVENT_ROTATE] = \&_v4_rotate_event;
    $$self{handlers}->[EVENT_RAND] = \&_v4_rand_event;
    $$self{handlers}->[EVENT_INTVAR] = \&_v4_intvar_event;
    $$self{handlers}->[EVENT_APPEND_BLOCK] = \&_v4_append_block_event;
    $$self{handlers}->[EVENT_USER_VAR] = \&_v4_user_var_event;
    $$self{handlers}->[EVENT_DELETE_FILE] = \&_delete_file_event;
    $$self{handlers}->[EVENT_WRITE_ROWS] = \&_v4_write_rows_event;
    $$self{handlers}->[EVENT_UPDATE_ROWS] = \&_v4_write_rows_event;
    $$self{handlers}->[EVENT_DELETE_ROWS] = \&_v4_write_rows_event;
    $$self{handlers}->[EVENT_TABLE_MAP] = \&_v4_table_map_event;
  }
  else {
    $$self{log_version} = 3;
    croak('Binlogs in v3 format not supported');
  }

}

sub seek {
  my ($self, $pos) = @_;
  unless(CORE::seek($$self{fh}, SEEK_SET, $pos)) {
    croak($!);
  }
  return 0;
}

sub read {
  my ($self) = @_;
  my $raw = '';
  my $buf;
  my $evt;
  my ($evt_time, $evt_type, $srv_id, $evt_len, $evt_next, $evt_flags);
  $_ = CORE::read($$self{fh}, $buf, $$self{header_length});
  if(defined($_) and $_ == 0) { # end of file reached
    return undef;
  }
  elsif(not defined($_)) {
    croak($!);
  }
  $raw .= $buf;

  if($$self{log_version} == 4) {
     ($evt_time, $evt_type, $srv_id, $evt_len, $evt_next, $evt_flags)
       = unpack('LCLLLS', $buf);
     $evt = _new_event($evt_time, $evt_type, $evt_len,
                       $srv_id, $evt_next, $evt_flags);
     if($evt_len - $$self{header_length} == 0) {
       return $evt;
     }

     $_ = CORE::read($$self{fh}, $buf, $evt_len - $$self{header_length});
     if(defined($_) and $_ == 0) {
       return undef;
     }
     elsif(not defined($_)) {
       croak($!);
     }
     $raw .= $buf;
     $$evt{data} = $buf;
     eval {
       &{$$self{handlers}->[$$evt{type}]}($evt, $raw);
     };
     if($@ and $@ =~ /Use of uninitialized value in subroutine entry/) {
       croak("No handler for event type $$evt{type}");
     }
     elsif($@) {
       croak($@);
     }
  }
  else {
    croak('Old log format');
  }

  return $evt;
}


sub _format_description_event {
  my ($self, $evt) = @_;
  my $fh = $$self{fh};
  my $buf;
  $$evt{server_version} = '';
  $$evt{create_timestamp} = -1;
  $$evt{header_length} = -1;
  $$evt{event_lengths} = [];

  CORE::read($fh, $buf, V4_HEADER_LEN-(V1_HEADER_LEN)) or croak($!);
  ($$evt{next_position}, $$evt{flags}) = unpack('LS', $buf);

  CORE::read($fh, $buf, $$evt{len} - V4_HEADER_LEN) or croak($!);

  ($$self{log_version}, $$evt{server_version},
   $$evt{create_timestamp}, $$evt{header_length})
    = unpack('Sa['. SERVER_VERSION_LEN .']LC', $buf);
  $$evt{server_version} =~ s/\0//g; # remove the null padding from the server_version field.

  $$evt{event_lengths} = [unpack('x[S]x['. SERVER_VERSION_LEN .']x[L]xC/C', $buf)];

  unshift @{$$evt{event_lengths}}, 0;
  unshift @{$$evt{event_lengths}}, 0;

  $$self{closed_properly} = !($$evt{flags} & LOG_EVENT_BINLOG_IN_USE_F);
}

sub _parse_status_variables {
  my ($evt, $stat_vars_len, $stat_vars) = @_;
  for(my $i = 0; $i < $stat_vars_len; $i++) {
    ($_) = unpack("x[$i]C", $stat_vars);
    if($_ == Q_FLAGS2_CODE) {
      ($$evt{flags2}) = unpack("x[$i]xL", $stat_vars);
      $i += 4;
    }
    elsif($_ == Q_SQL_MODE_CODE) {
      ($$evt{sql_mode}) = unpack("x[$i]xQ", $stat_vars);
      $i += 8;
    }
    elsif($_ == Q_CATALOG_CODE or $_ == Q_CATALOG_NZ_CODE) {
      my $len;
      ($len, $$evt{catalog_code}) = unpack("x[$i]xCXC/a", $stat_vars);
      $i += $len+1;
    }
    elsif($_ == Q_CHARSET_CODE) {
      ($$evt{character_set_client},
       $$evt{collation_connection},
       $$evt{collation_server}) = unpack("x[$i]xSSS", $stat_vars);
      $i += 6;
    }
    elsif($_ == Q_AUTO_INCREMENT) {
      ($$evt{auto_increment_increment},
       $$evt{auto_increment_offset}) = unpack("x[$i]xSS", $stat_vars);
    }
    elsif($_ == Q_TIME_ZONE_CODE) {
      my $len;
      ($len, $$evt{timezone}) = unpack("x[$i]xCXC/a", $stat_vars);
      $i += $len+1;
    }
    elsif($_ == Q_LC_TIME_NAMES_CODE) {
      ($$evt{lc_time_names}) = unpack("x[$i]xS", $stat_vars);
      $i += 2;
    }
    elsif($_ == Q_CHARSET_DATABASE_CODE) {
      ($$evt{database_charset}) = unpack("x[$i]xS", $stat_vars);
      $i += 2;
    }
    elsif($_ == Q_TABLE_MAP_FOR_UPDATE_CODE) {
      ($$evt{table_map_for_update_bitmap}) = unpack("x[$i]xQ", $stat_vars);
      $i += 8;
    }
    else {
      croak("Unknown status variable $_");
    }
  }
}

sub _unpack_int {
  my ($bytes) = @_;
  my $u; # consumed bytes
  ($_) = unpack('C', $bytes);
  if($_ == 252) {
    ($_) = unpack('x[C]S', $bytes);
    $u = 3;
  }
  elsif($_ == 253) {
    ($_) = unpack('L', $bytes);
    $_ &= 0x00ffffff;
    $u = 4;
  }
  elsif($_ == 254) {
    ($_) = unpack('x[C]Q', $bytes);
    $u = 9;
  }
  else {
    $u = 1;
  }
  return ($u, $_);
}

sub _v4_query_event {
  my ($evt) = @_;
  my ($stat_vars_len, $stat_vars, $db_len);
  ($$evt{thread_id}, $$evt{exec_time},
   $db_len, $$evt{error_code}, $stat_vars_len) = unpack('LLCSS', $$evt{data});
  ($stat_vars) = unpack("x[LLCSS]a[$stat_vars_len]", $$evt{data});
  _parse_status_variables($evt, $stat_vars_len, $stat_vars);
  ($$evt{database}, $$evt{stmt}) = unpack("x[LLCSS]x[$stat_vars_len]Z*a*", $$evt{data});
  delete $$evt{data};
}


sub _v4_rotate_event {
  my ($evt) = @_;
  ($$evt{rotate_pos}) = unpack('Q', $$evt{data});
  ($$evt{rotate_file}) = unpack('x[Q]a*', $$evt{data});
  delete $$evt{data};
}

sub _v4_intvar_event {
  my ($evt) = @_;
  ($$evt{intvar_type}, $$evt{intvar_value}) = unpack('CQ', $$evt{data});
  delete $$evt{data};
}

sub _v4_append_block_event {
  my ($evt) = @_;
  ($$evt{file_id}, $$evt{file_data}) = unpack('La*', $$evt{data});
  delete $$evt{data};
}

sub _delete_file_event {
  my ($evt) = @_;
  ($$evt{file_id}) = unpack('L', $$evt{data});
  delete $$evt{data};
}

sub _v4_rand_event {
  my ($evt) = @_;
  ($$evt{rand_seed1}, $$evt{rand_seed2}) = unpack('QQ', $$evt{data});
  delete $$evt{data};
}

sub _v4_user_var_event {
  my ($evt) = @_;
  ($$evt{variable_name}, $$evt{variable_null},
   $$evt{variable_type}, $$evt{variable_character_set},
   $$evt{variable_length}, $$evt{variable_value}) = unpack('L/aCCLLa*', $$evt{data});
  use Data::Dumper;
  if($$evt{variable_null} == 0) {
    if($$evt{variable_type} == U_INT_RESULT) {
      ($$evt{variable_value}) = unpack('Q', $$evt{variable_value});
    }
    elsif($$evt{variable_type} == U_REAL_RESULT) {
      ($$evt{variable_value}) = unpack('f', $$evt{variable_value});
    }
    elsif($$evt{variable_type} == U_DECIMAL_RESULT) {
      ($$evt{variable_value}) = unpack('d', $$evt{variable_value});
    }
  }
  delete $$evt{data};
}

sub _v4_xid_event {
  my ($evt) = @_;
  ($$evt{xid}) = unpack('Q', $$evt{data});
  delete $$evt{data};
}


sub _v4_execute_load_query_event {
  my ($evt) = @_;
  my ($stat_vars_len, $stat_vars, $db_len);
  ($$evt{thread_id}, $$evt{exec_time},
   $db_len, $$evt{error_code}, $stat_vars_len,
   $$evt{file_id}, $$evt{file_name_start},
   $$evt{file_name_end}, $$evt{dup_handling}
  ) = unpack('LLCSSLLLC', $$evt{data});
  ($stat_vars) = unpack("x[LLCSSLLLC]a[$stat_vars_len]", $$evt{data});
  _parse_status_variables($evt, $stat_vars_len, $stat_vars);
 ($$evt{database}, $$evt{stmt}) = unpack("x[LLCSSLLLC]x[$stat_vars_len]Z*a*", $$evt{data});
  delete $$evt{data};
}

sub _hex_dump {
  my ($mem) = @_;
  my $i = 0;
  map( ++$i % 16 ? $_." " : $_ ."\n",
             unpack( 'H2' x length( $mem ), $mem ) ),
               length( $mem ) % 16 ? "\n" : '';
}

sub _v4_table_map_event {
  my ($evt, $raw) = @_;
  my ($s1, $s2, $s3, $i);
  ($s1, $s2, $s3, $$evt{reserved_flags}, $$evt{database}, $$evt{table}, $i)
    = unpack('SSSSxZ*xZ*a[9]', $$evt{data});
  $$evt{table_id} =  $s1 + (($s2 << 16) + ($s3 << 32));
  ($_, $i) = _unpack_int($i);
  $s1 = $_ + length($$evt{database}) + length($$evt{table})+12;
  $$evt{column_types} = [unpack("x[$s1]C[$i]", $$evt{data})];
  $s1 += $i;
  ($i) = unpack("x[$s1]a[9]", $$evt{data});
  ($_, $i) = _unpack_int($i);

  $$evt{data} = encode_base64($raw);
}

sub _v4_write_rows_event {
  my ($evt, $raw) = @_;
  $$evt{data} = encode_base64($raw);
}

1;
# ###########################################################################
# End MysqlBinlogParser package
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

package pdb_zrm_restore;
use strict;
use warnings;
use Getopt::Long qw(:config no_ignore_case);
use Data::Dumper;
use Pod::Usage;
use DBI;
use File::Spec qw(splitdir);
use File::Path;
use File::Basename;
use Sys::Hostname;


my $pl;

sub main {
  @ARGV = @_;
  my %o;
  $o{'mysql-user'} = 'root';
  $o{'mysql-password'} = '';
  $o{'log-file'} = '/dev/null';

  # Locate our various external programs.
  $o{'mysqlbinlog'} = Which::which('mysqlbinlog');
  $o{'mysql'} = Which::which('mysql');
  $o{'innobackupex'} = Which::which('innobackupex-1.5.1');
  $o{'mysqld'} = Which::which('mysqld_safe');

  my @backups = ();
  my %cfg;
  my $datadir;
  GetOptions(\%o,
    'help' => sub { pod2usage(-verbose => 99) },
    'dry-run',
    'log-file|L=s',
    'identify-dirs|i',
    'estimate|e',
    'defaults-file|F=s',
    'mysql-user=s',
    'mysql-password=s',
    'reslave|r',
    'mysqld=s',
    'mysql=s',
    'mysqlbinlog=s',
    'innobackupex=s',
    'slave-user=s',
    'slave-password=s',
    'master-host=s',
    'rel-base|b=s',
    'strip|p=s',
    'point-in-time|t=s',
    'create-dirs',
    'skip-extract',
    'force|f',
  );

  $pl = ProcessLog->new($0, $o{'log-file'}, undef);
  $pl->d('ARGV:', @ARGV);
  if(not exists $o{'identify-dirs'} and exists $o{i}) {
    $o{'identify-dirs'} = $o{i};
  }
  if(not exists $o{'defaults-file'} and exists $o{F}) {
    $o{'defaults-file'} = $o{F};
  }
  if(not exists $o{'estimate'} and exists $o{e}) {
    $o{'estimate'} = $o{e};
  }
  if(not exists $o{'rel-base'} and exists $o{b}) {
    $o{'rel-base'} = $o{b};
  }
  if(not exists $o{'strip'} and exists $o{p}) {
    $o{'strip'} = $o{p};
  }
  if(not exists $o{'point-in-time'} and exists $o{t}) {
    $o{'point-in-time'} = $o{p};
  }
  if(not exists $o{'force'} and exists $o{f}) {
    $o{'force'} = $o{f};
  }
  if(!$o{'identify-dirs'} and !$o{'defaults-file'}) {
    $pl->e('Must have --defaults-file or --identify-dirs at a minimum. Try --help.');
    return 1;
  }

  # Ensure that --mysqld points to the 'safe' shell script.
  if(not exists $o{'identify-dirs'} and (!$o{'mysqld'} or $o{'mysqld'} !~ /safe/)) {
    $pl->e('You must provide a path to mysqld_safe, *not* the raw binary. Try --help.');
    return 1;
  }

  # Collect all the backup set information straight away.
  eval {
    my $backup = ZRMBackup->new($pl, $ARGV[0]);
    @backups = $backup->find_full($o{'strip'}, $o{'rel-base'});
  };
  if($@ and $@ =~ /No full backup/) {
    $pl->e("Unable to find full backup for this backup-set.");
    return 1;
  }
  elsif($@) {
    chomp($@);
    $pl->e("Could not find any backups: $@");
    return 1;
  }

  # If we're just identifying dirs, print them out.
  if($o{'identify-dirs'}) {
    foreach my $b (@backups) {
      print $b->backup_dir, "\n";
    }
  }
  return 0 if($o{'identify-dirs'});

  # We must be doing an actual restore.
  if(!$o{'defaults-file'}) {
    $pl->e("Must specify --defaults-file for restore.");
    return 1;
  }

  %cfg = read_config($o{'defaults-file'});
  $datadir = $cfg{'mysqld'}{'datadir'};

  if($o{'create-dirs'}) {
    eval {
      mkpath($datadir);
      if(exists $cfg{'mysqld'}{'log-bin'} and $cfg{'mysqld'}{'log-bin'} =~ m|^/|) {
        mkpath(dirname($cfg{'mysqld'}{'log-bin'}));
      }
    };
    if($@) {
      $pl->e("Unable to create directories:", $datadir, $cfg{'mysqld'}{'log-bin'}, "\n", "exception:", $@);
      return 1;
    }
  }

  unless( -d $datadir ) {
    $pl->e("Datadir doesn't exist.");
    return 1;
  }

  unless( -w $datadir ) {
    $pl->e("Cannot write to the datadir. Are you the right user?");
    return 1;
  }

  # Prepare an estimate and wait for enter
  # if we're not doing a dry run and --estimate was given.
  if(!$o{'dry-run'} && $o{'estimate'}) {
    make_estimate(@backups);
    $_ = <STDIN>; # Wait for enter.
  }

  # Remove the datadir, just in case it was
  # being used like a scratch area.
  unless($o{'dry-run'}) {
    unless($o{'skip-extract'}) {
      $pl->m("Removing contents of mysqld.datadir:", $datadir);
      Path::dir_empty($datadir);
      if($cfg{'mysqld'}{'log-bin'} and $cfg{'mysqld'}{'log-bin'} =~ m|^/|) {
        $pl->m("Removing contents of mysqld.log-bin:", dirname($cfg{'mysqld'}{'log-bin'}));
        Path::dir_empty(dirname($cfg{'mysqld'}{'log-bin'}));
      }
    }
    else {
      $pl->i("Skipping emptying $datadir due to --skip-extract");
    }
  }

  # Extract the backups
  if(extract_backups(\%o, $datadir, @backups)) {
    $pl->e("Bailing due to extraction errors.");
    return 1;
  }

  if( -f "$datadir/xtrabackup_logfile" ) {
    $pl->m("Applying xtrabackup log.");
    unless($o{'dry-run'}) {
      my %r = %{$pl->x(sub { system(@_) }, "cd $datadir ; $o{'innobackupex'} --defaults-file=$o{'defaults-file'} --apply-log .")};
      if($r{rcode} != 0) {
        $pl->e("Error applying xtrabackup log:");
        $_ = $r{fh};
        while (<$_>) { $pl->e($_); }
        $pl->e("Bailing out.");
        return 1;
      }
    }
  }
  else {
    $pl->m("Target doesn't look like an xtrabackup, not attempting log apply.");
  }

  my $iblog_size = $cfg{'mysqld'}{'innodb_log_file_size'};
  if(defined $iblog_size) {
    # Convert to size in bytes
    if($iblog_size =~ /(\d+)[Mm]$/) {
      $iblog_size = $1*1024*1024;
    }
    elsif($iblog_size =~ /(\d+)[Gg]$/) {
      $iblog_size = $1*1024*1024*1024;
    }
    if(-s "$datadir/ib_logfile0" < $iblog_size or -s "$datadir/ib_logfile0" > $iblog_size) {
      $pl->i("ib_logfiles are not the size that $o{'defaults-file'} says they should be.");
      $pl->i("Removing the ib_logfiles.");
      unlink(<$datadir/ib_logfile*>);
    }
  }

  if($backups[-1]->backup_level == 1) {
    start_mysqld(\%o, \%cfg);

    # Get binlog positions, and pipe into mysql command
    $pl->m("Applying binlogs.");
    unless($o{'dry-run'}) {
      $pl->m('Reading position information from', $datadir . '/xtrabackup_binlog_info');
      open BINLOG_INFO, '<', "$datadir/xtrabackup_binlog_info";
      my $l = <BINLOG_INFO>;
      close BINLOG_INFO;
      my ($binlog, $pos) = split(/\s+/, $l);
      my ($first_fname, $first_logno) = split( '\.', $binlog);
      my $binlog_pattern = $backups[-1]->incremental();
      my @logs = ();
      my $binlog_opts = '';
      my $mysql_opts = '';
      for(sort(<$datadir/$binlog_pattern>)) {
        my ($fname, $logno) = split('\.', $_);
        if(int($first_logno) > int($logno)) {
          $pl->d('Skipping binlog:', $_);
          next;
        }
        if(int($first_logno) == int($logno)) {
          $pl->d('First binlog after backup point.');
          $binlog_opts = "--start-position=$pos";
        }

        push @logs, $_;
      }
      if($o{'point-in-time'}) {
        $pl->d("Adding --stop-datetime='". $o{'point-in-time'} ."' due to --point-in-time on commandline.");
        $binlog_opts .= " --stop-datetime='$o{'point-in-time'}'";
      }
      if($o{'force'}) {
        $pl->d("Forcing binlog apply, even in the face of errors.");
        $mysql_opts = "--force";
      }
      $pl->m('Applying:', @logs);
      $_ = join(' ', @logs);
      $pl->d("exec: $o{'mysqlbinlog'} $binlog_opts $_ | $o{'mysql'} --defaults-file=$o{'defaults-file'} $mysql_opts");
      system("$o{'mysqlbinlog'} $binlog_opts $_ | $o{'mysql'} --defaults-file=$o{'defaults-file'} $mysql_opts");
      if(($? >> 8) > 0) {
        stop_mysqld(\%o, \%cfg);
        $pl->e('Error applying binlog.');
        return 1;
      }
    }
    stop_mysqld(\%o, \%cfg);
    wait;
  }

  if($o{'dry-run'}) { make_estimate(@backups); }

  return 0;
}

if ( !caller ) { exit main(@ARGV); }

sub start_mysqld {
  my ($o,  $cfg) = @_;
  my %o = %$o;
  my %cfg = %$cfg;
  my $pid = fork;
  if($pid == 0) {

    if($cfg{'mysqld_safe'}{'user'} and $cfg{'mysqld_safe'}{'group'}) {
      $pl->i('attempting to chown', $cfg{'mysqld'}{'datadir'}, 'to',  "$cfg{'mysqld_safe'}{'user'}:$cfg{'mysqld_safe'}{'group'}");
      system("chown -R $cfg{'mysqld_safe'}{'user'}:$cfg{'mysqld_safe'}{'group'} $cfg{'mysqld'}{'datadir'}");

      if($cfg{'mysqld'}{'log-bin'}) {
        $pl->i('attempting to chown', dirname($cfg{'mysqld'}{'log-bin'}), 'to',  "$cfg{'mysqld_safe'}{'user'}:$cfg{'mysqld_safe'}{'group'}");
        system("chown -R $cfg{'mysqld_safe'}{'user'}:$cfg{'mysqld_safe'}{'group'} ". dirname($cfg{'mysqld'}{'log-bin'}));
      }
    }


    my @path = File::Spec->splitdir($o{'mysqld'});
    pop @path; pop @path;
    my $mysqld_basedir = File::Spec->catdir(@path);
    $pl->i('mysqld basedir:', $mysqld_basedir);
    $pl->i('starting mysqld with:', $o{'mysqld'}, '--defaults-file='. $o{'defaults-file'}, '--skip-grant-tables', '--skip-networking');
    chdir $mysqld_basedir;
    unless($o{'dry-run'}) {
      exec "$o{'mysqld'} --defaults-file=$o{'defaults-file'} --skip-grant-tables --skip-networking"
    }
    else {
      exit(0);
    }
  }
  elsif(not defined $pid) {
    $pl->e('Unable to spawn mysqld:', $!);
    return undef;
  }
  else {
    # Makes sure mysqld has started completely before giving
    # control over to other code.
    unless($o{'dry-run'}) {
      while(read_pidfile($cfg{'mysqld'}{'pid-file'}) !~ /\d+/) { sleep 1; }
    }
    else { # This is so the log looks correctly ordered on --dry-run.
      sleep(1);
    }
  }

  return 0;
}

sub stop_mysqld {
  my ($o, $cfg) = @_;
  my %o = %$o;
  my %cfg = %$cfg;
  my $r = 0;
  $pl->i('killing mysqld with -15');
  unless($o{'dry-run'}) {
    $r = kill 15, read_pidfile($cfg{'mysqld'}{'pid-file'});
  }
  return $r;
}

sub make_estimate {
  my @backups = @_;
  my $kbytes = 0.0;
  foreach my $bk (@backups) {
    $kbytes += $bk->backup_level == 1 ? 5.0*$bk->backup_size : $bk->backup_size;
  }
  $pl->i("Space estimate (MB):", $kbytes/1024.0);
  return 0;
}

sub read_pidfile {
  my $pidfile = shift;
  my $pid;
  open my $fh, "<$pidfile" or return "";
  chomp($pid = <$fh>);
  close($fh);
  return $pid;
}

sub extract_backups {
  my ($o, $ddir, @backups) = @_;
  my %o = %$o;
  if($o{'skip-extract'}) {
    $pl->i("Skipping backup extraction due to --skip-extract");
    return 0;
  }
  $pl->m("Extracting backups to $ddir");
  my ($r, $fh) = (0, undef);
  foreach my $bk (@backups) {
    $pl->m("Extracting", $bk->backup_dir);
    unless( $o{'dry-run'} ) {
      ($r, $fh) = $bk->extract_to($ddir);
      if($r != 0) {
        $pl->e("Extraction errors:");
        while(<$fh>) { $pl->e($_); }
      }
      close($fh);
    }
  }
  return $r;
}

# Loads a my.cnf into a hash.
# of the form:
# key: group
# val: { <option> => <value> }
# Strips spaces and newlines.
sub read_config {
  my $file = shift;
  my %cfg = IniFile::read_config($file);
  unless(%cfg) {
    $pl->e("Unable to open defaults file: $file. Error: $!");
  }
  unless($cfg{'mysqld'}{'pid-file'}) {
    if($cfg{'mysqld_safe'}{'pid-file'}) {
      $cfg{'mysqld'}{'pid-file'} = $cfg{'mysqld_safe'}{'pid-file'};
    }
    else {
      $cfg{'mysqld'}{'pid-file'} = $cfg{'mysqld'}{'datadir'} .'/'. hostname() . '.pid';
    }
  }
  return %cfg;
}

1;

__END__

=head1 NAME

pdb-zrm-restore - Do a point-in-time restore from a zrm backup.

=head1 RISKS

This section is here to inform you that this tool may have bugs.
In general, this tool should be safe, provided that you do not test
it out in production. At the time of this release, there are no known
bugs, but that does not mean there are none.

It's completely possible to shoot yourself in the foot at this time.
This tool does no checking whatsoever to make sure you don't overwrite
an active mysql datadir. It B<will> just blindly empty it and assume it's got control.

=head1 SYNOPSIS

pdb-zrm-restore --defaults-file /etc/my.cnf [last backup directory]

=head1 ARGUMENTS

The only non-option argument passed to pdb-zrm-restore is a path
to a zrm backup directory. If the directory points to an incremental
backup, pdb-zrm-restore will walk backwards till it finds a full backup.

=head1 OPTIONS

=over 4

=item --help,-h

This help.

=item --dry-run

Report on actions that would be taken, and print an estimate of how much disk
space will be needed for the restore.

=item --log-file,-L

Sets the logfile that should be written to.

Default: /dev/null

=item --identify-dirs,-i

Rather than restoring, simply list all directories
up to the most recent full.

=item --estimate,-e

Estimate the space required by the restore and wait for enter to be pressed.

=item --defaults-file,-F

Use mysqld options from this file. In particular, pdb-zrm-restore
needs this option to determine WHERE to restore.

=item --create-dirs

pdb-zrm restore will create the path specified by mysql.datadir
in found in L<--defaults-file>.

=item --skip-extract

Instead of doing the whole extraction cycle, just apply the xtrabackup log,
and replay binlogs identified by the backup sets. This prevents pdb-zrm-restore
from emptying the contents of the datadir. Mysql must still not be running.

=item --rel-base,-b

If you've copied the backup data from another host,
and are unable or uninterested in duplicating the same directory
structure as on the backup server. This option causes pdb-zrm-restore
to look for backups relative to this directory. See the below example.

  On backup server: /mysqlbackups/<backup-set>/<datestamp>
  Using --rel-base /srv: /srv/mysqlbackups/<backup-set>/<datestamp>

This flag is often needed since this tool automaticallly tracks back
to the most recent full backup from the backup provided on the commandline.

This flag is always applied AFTER L<--strip> to help you readjust
the lookup path for backups.

Default: (none)

=item --strip,-p

If the value looks like a number, then this flag strips N path components
off the front of the backup-set directories. See the below example.

  backup-set dir from index: /mysqlbackups/<backup-set>/<datestamp>
  Using --strip 1: /<backup-set>/<datestamp>

Otherwise, it's assumed to be a leading path (starting with '/') to be
stripped off. See below:

  backup-set dir from index: /some/deep/path/<backup-set>/<datestamp>
  Using --strip /some/deep/path: /<backup-set>/<datestamp>

This flag is always applied BEFORE L<--rel-base> so that you can
readjust the lookup path for backups to suit your needs.

Default: 0

=item --point-in-time,-t

Apply binlogs up to an exact date. If there isn't a binlog
entry for the specific time given, logs will be applied until
as close as possible, but not past that time.

The date given must be in the format: C<YYYY-MM-DD HH:mm:SS>
Quoting to protect the space from the shell is likely necessary.

=item --force,-f

Continue even after errors when applying binlogs.

=item --mysqld

Use this mysqld binary to start up the server for binlog replay
and reslaving configuration. This B<must> be the path to mysqld_safe.

Default: `which safe_mysqld`

=item --innobackupex

Use this to specify the full path to innobackupex, if it not in your path.

Default: `which innobackupex-1.5.1`


=back
