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

package pdb_sandbox_merge;
use strict;
use warnings;

use Getopt::Long qw(:config no_ignore_case);
use Data::Dumper;
use List::Util;


$Data::Dumper::Indent = 1;

my $pl;

sub main {
  @ARGV = @_;
  my %o;
  my $base;
  my @pids;
  $o{'ignore-db'} = ();
  $o{'parallel'} = 1;
  $o{'user'} = 'root';
  $o{'password'} = 'msandbox';
  $o{'log-file'} = '/dev/null';
  @{$o{'ignore-db'}} = qw(mysql information_schema);
  GetOptions(\%o,
    'help|h',
    'user|u=s',
    'log-file|L=s',
    'password|p:s',
    'ignore-db|i=s@',
    'include-mysql-db|I',
    'force',
    'parallel|P!',
  );
  my @sandboxes = @ARGV;
  $pl = ProcessLog->new($0, $o{'log-file'}, undef);

  if(@sandboxes < 2) {
    $pl->e('Cannot merge a single sandbox. That makes no sense.');
    return 1;
  }

  foreach my $sbox (@sandboxes) {
    unless( -d $sbox and -f "$sbox/my.sandbox.cnf" ) {
      $pl->e($sbox, 'does not look like a mysql sandbox.');
      return 1;
    }
  }

  $pl->d("options:\n", Dumper(\%o));

  # Shift off the first sandbox, where all the others will write.
  $base = shift @sandboxes;

  foreach my $sbox (@sandboxes) {
    my $pid=fork();
    if($pid == 0) {
      my %cfg = IniFile::read_config("$sbox/my.sandbox.cnf");
      if(not %cfg) {
        $pl->e("Unable to read $sbox/my.sandbox.cnf");
        exit(1);
      }
      my @dbs = qx|$sbox/use --user=$o{'user'} --password='$o{'password'}' --batch --skip-column-names -e 'show databases'|;
      chomp(@dbs);
      @dbs = grep { !/mysql/ } @dbs;
      @dbs = grep { !/information_schema/} @dbs;
      @dbs = grep {
        my $a = $_;
        # That is: Make sure the db is not equal to any of the
        # listed ignores.
        any($a, @{$o{'ignore-db'}});
      } @dbs;
      my $load_force = $o{'force'} ? '--force' : '';
      $pl->d('dump command:', qq#$sbox/my sqldump --routines --no-autocommit --skip-add-drop-table --skip-add-drop-database --add-locks --extended-insert --quick --user=$o{'user'} --password='$o{'password'}' --socket=$cfg{'mysqld'}{'socket'} --databases @dbs | $base/use $load_force --user=$o{'user'} --password='$o{'password'}'#);

      exec("$sbox/my sqldump --routines --no-autocommit --skip-add-drop-table --skip-add-drop-database --add-locks --extended-insert --quick --user=$o{'user'} --password='$o{'password'}' --socket=$cfg{'mysqld'}{'socket'} --databases @dbs | $base/use $load_force --user=$o{'user'} --password='$o{'password'}'");
    }
    elsif(defined $pid and $pid > 0) {
      $pl->i("Spawned mysqldump for $sbox.");
      push @pids, $pid;
    }
    else {
      $pl->e("An error occurred while spawning a mysqldump for: $sbox");
      $pl->e("The merge has been aborted.");
      kill -15, @pids;
      1 while(wait != -1);
      return 1;
    }
    # If --noparallel, then wait for each.
    if(!$o{'parallel'}) { wait; }
  }

  # After we've spawned all our "threads",
  # Then we wait for them all.
  while(wait != -1) {
    if( ($? >> 8) != 0 ) {
      $pl->e("One of the loads did not complete successfully. It returned: $?");
      $pl->e("Aborting rest of dumps.");
      kill_all(@pids);
    }
  }

  return 0;
}

if( !caller ) { exit main(@ARGV); }

sub kill_all {
  kill -15, @_;
}

sub any {
  my $a = shift;
  $_ ne $a || return 0 for @_; 1;
}

1;
__END__

=head1 NAME

pdb-sandbox-merge - Merge N MysqlSandboxes into one.

=head1 RISKS

This section is here to inform you that this tool may have bugs.
In general, this tool should be safe, provided that you do not test
it out in production. At the time of this release, there are no known
bugs, but that does not mean there are none.

=head1 SYNOPSIS

pdb-sandbox-merge ~/sandboxes/msb1 ~/sandboxes/msb2 ... ~/sandboxes/msbN

=head1 ARGUMENTS

A list of paths to sandboxes should be provided either before or after
all options. pdb-sandbox-merge will exit if any of the provided paths
do not look like a mysql sandbox.

This tool only operates on local mysqlsandboxes. Support for remote mysql
servers is not currently a planned feature.

=head1 CONFLICT HANDLING

There are several ways conflicts during the merge can be handled.

=over 4

=item B<ignore>

Conflicts during the merge are simply ignored and data is trashed.

=item B<die>

The first conflict causes pdb-sandbox-merge to die. B<This is the default.>

=back

At some later time, the following strategies will be implemented:

=over 4

=item B<database>

Databases with the same name are skipped in all sandboxes except the first.

=item B<table>

Tables in the same database with the same name are skipped in all but the first sandbox.

=item B<row>

The merge is aborted on the first row constraint failure. That is, an attempt to insert a duplicate primary key, or, a unique key fails.

=back

=head1 OPTIONS

=over 4

=item --user,-u

User across all sandboxes.

Default: root

=item --password,-p

Password accross all sandboxes.

Default: msandbox

=item --log-file,-L

Where to log output from this tool.

Default: /dev/null

=item --ignore-db,-i

May be specified many times. Causes the named database to not be merged.
The default is mysql, and additional calls to this option will add to that, not overwrite. See L<--include-mysql-db> to have mysql db merged.

Default: mysql

=item --include-mysql-db,-I

Causes the mysql db to be merged. The mysql db is not normally merged.

=item --parallel,-P

Do the merge in parallel. This is the default.

=back
