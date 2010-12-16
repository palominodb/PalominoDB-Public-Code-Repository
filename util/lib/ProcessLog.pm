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
package ProcessLog;
use strict;
use warnings FATAL => 'all';

=pod

=head1 NAME

ProcessLog - Logging framework like Log4j but different

=head1 SYNOPSIS

  # New ProcessLog
  my $pl = ProcessLog->new($0, 'some.log');

  # Regular message
  $pl->m('my', 'message');

  # Info message
  $pl->i('info', 'message');

  # Error message
  $pl->e('error');

  # Debug message (only if $ENV{PDB_DEBUG} is set)
  $pl->d('debugging', 'output');

  # Error message with a stack trace
  $pl->es('other error');

=head1 METHODS

=cut

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

=pod

=head3 C<new($script_name, $logpath, $email_to)>

Creates a new processlog.

C<$script_name>: the name of the program using this. Normally $0.
C<$logpath>: filename of log. Normally $0.
If it matches C<< syslog:<facility> >>, then output is sent to syslog.

=cut

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
  if($logpath =~ /^syslog:(\w+)/) {
    openlog($script_name, "", $1);
    $self->{logsub} = sub {
      my $self = shift;
      my $lvl = 'LOG_DEBUG';
      $lvl = 'LOG_INFO' if($_[0] eq "msg");
      $lvl = 'LOG_NOTICE' if($_[0] eq "ifo");
      $lvl = 'LOG_ERR'  if($_[0] eq "err");
      foreach my $l (split "\n", _p(@_)) {
        syslog($lvl, $l);
      }
      print _p(@_) unless $self->{quiet};
    };
  }
  elsif($logpath eq 'pdb-test-harness' or $logpath eq 'stderr') {
    $self->{logsub} = sub {
      my $self = shift;
      print STDERR '# ', _p(@_);
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

  bless $self,$class;
  return $self;
}

=pod

=head3 C<null()>

Create a null processlog, useful for nagios plugins.
This processlog object writes log messages to /dev/null.
See L<quiet()> for disabling writes to stdout.

=cut

sub null {
  my $class = shift;
  $class->new('', '/dev/null', undef);
}

=pod

=head3 C<name()>

Returns the name given to this ProcessLog
when it was created.

=cut

sub name {
  my $self = shift;
  $self->{script_name};
}

=pod

=head3 C<runid()>

Returns the runid of this processlog.
If you want a new runid - make a new processlog.

RunIDs are SHA1's of the script name, the time
with microseconds, and some random value. The objective
is to produce something that should be unique.
UUID's might be used in the future, so don't depend on
this being a SHA1.

=cut

sub runid {
  my $self = shift;
  $self->{run_id};
}

=pod

=head3 C<start()>

Logs a message like C<< 'BEGIN <runid>' >>.
This is so that a tool can log to the same
log file and there's a clear dileneation between runs.

=cut

sub start {
  my $self = shift;
  $self->m("BEGIN $self->{run_id}");
}

=pod

=head3 C<end()>

Logs an 'end' message. See L<start()>.
End messages look like: C<< 'END <runid>' >>.

=cut

sub end {
  my $self = shift;
  $self->m("END $self->{run_id}");
}

=pod

=head3 C<stack_depth()>

Gets/Sets the maximum depth for stack traces produced by L<stack()>.

=cut

sub stack_depth {
  my ($self, $opts) = @_;
  my $old = $self->{stack_depth};
  $self->{stack_depth} = $opts if( defined $opts );
  $old;
}

=pod

=head3 C<quiet()>

Gets/Sets whether or not this ProcessLog will log to stdout.

=cut

sub quiet {
  my ($self, $new) = @_;
  my $old = $self->{quiet};
  $self->{quiet} = $new if( defined $new );
  $old;
}

=pod

=head3 C<m(@args)>

Regular message.

=cut

sub m {
  my ($self,$m) = shift;
  my $fh = $self->{LOG};
  my $t = sprintf("%.3f", time());
  $self->{logsub}->($self, 'msg', undef, undef, $t, @_);
}

=pod

=head3 C<ms(@args)>

Regular message with a stack trace.

=cut

sub ms {
  my $self = shift;
  $self->m(@_);
  $self->m($self->stack());
}

=pod

=head3 C<p()>

Prompt the user for a value, and record the result in the log.

Accepts either: C<strings ...[, regex[, default]]>,
or, C<filehandle, strings ...[, regex[, default]]>.
In the general case you should always prompt from C<STDIN> and never use
the second form, but, the second form could be used for interactive
network servers or testing purposes.

The regex is to make sure that the input matches the caller's expectations.
C<p()> will continue to prompt indefinitely as long as the
input does not match the regex. If no input is given, and
a default value was specified, then C<p()> will return that instead
of prompting again.

=cut

sub p {
  my ($self) = shift;
  my $fh = \*STDIN;
  my $regex = qr/.*/;
  my $default = undef;
  my @prompt = ();
  # First arg is a filehandle
  if(ref($_[0]) eq 'GLOB') {
    $fh = shift;
  }
  # Last arg is a regexp, no default
  if(ref($_[-1]) eq 'Regexp') {
    $regex = pop;
  }
  # Last arg is not a regexp, second to last is
  # last arg is assumed to be a default
  elsif(ref($_[-2]) eq 'Regexp') {
    $default = pop;
    $regex = pop;
  }
  @prompt = @_;
  # Log all string input
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

=pod

=head3 C<e(@args)>

Error message.

=cut

sub e {
  my ($self,$m) = shift;
  my ($package, undef, $line) = caller 0;
  my $fh = $self->{LOG};
  my $t = sprintf("%.3f", time());
  $self->{logsub}->($self, 'err', $package, $line, $t, @_);
}

=pod

=head3 C<e($die,@args)>

Error message and die with first argument.
Useful for errors that may be capturable later.

=cut

sub ed {
  my ($self) = shift;
  $self->e(@_);
  die(shift(@_) . "\n");
}

=pod

=head3 C<es(@args)>

Error message with a stack trace.

=cut

sub es {
  my $self = shift;
  $self->e(@_);
  $self->e($self->stack());
}

=pod

=head3 C<i(@args)>

Log some information.

=cut

sub i {
  my $self = shift;
  my $fh = $self->{LOG};
  my $t = sprintf("%.3f", time());
  $self->{logsub}->($self, 'ifo', undef, undef, $t, @_);
}

=pod

=head3 C<is(@args)>

Log some information with a stack trace.

=cut

sub is {
  my $self = shift;
  $self->i(@_);
  $self->i($self->stack());
}

=pod

=head3 C<d(@args)>

Log debugging output.

=cut

sub d {
  my $self = shift;
  my ($package, undef, $line) = caller 0;
  my $fh = $self->{LOG};
  if(_PdbDEBUG) {
    my $t = sprintf("%.3f", time());
    $self->{logsub}->($self, 'dbg', $package, $line, $t, @_);
  }
}

=pod

=head3 C<ds(@args)>

Log debugging output with a stack trace.

=cut

sub ds {
  my $self = shift;
  $self->d(@_);
  $self->d($self->stack());
}

=pod

=head3 C<x($coderef, @args)>

Execute a perlsub redirecting stdout/stderr
to an anonymous temporary file.
This is useful for wrapping an external tool.

B<WARNING:> This function is B<NOT> thread-safe.
But, it should be fork() safe.

B<NOTE:> Due to the way C<system()> is implemented
it's not possible to pass it as a coderef, you must
wrap it in a dummy subroutine - anonymous or otherwise.

=cut

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
  # Restore the filehandles out here
  # since, we may be wrapping a sub that calls 'exit' at some point.
  $self->_restore_stdfhs($saved_fhs);
  # Rewind the filehandle to the beginning to allow the calling application
  # to deal with it.
  seek($proc_fh, 0, SEEK_SET); 
  return {rcode => $r, error => $EVAL_ERROR . $self->stack, fh => $proc_fh};
}

=pod

=head3 C<stack()>

Returns a stack trace as a string.
Nicely indented for easy viewing.

=cut

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
  my $prefix = "$mode ";
  $prefix .= "${package}:${line} " if(defined $package and defined $line);
  $prefix .= "$time: ";
  @_ = map { (my $temp = $_) =~ s/\n/\n$prefix/g; $temp; }
       map { defined $_ ? $_ : 'undef' } @_;
  $prefix. join(' ',@_). "\n";
}

# To support the test harness, mainly
# Presently not used by any public functions.
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

=pod

=head3 C<email_and_die($extra)>

Send an email to the pre-defined location.
If no email was specified at creation time, this method does nothing.
Accepts an argument $extra which is intended to be a summary of the problem.

The format of the email is as follows:
    Subject: <script_name> FAILED

    <script_name> on <hostname> failed at <time>.
    The Error: <extra>
    <stack trace>
    RUN ID (for grep): <runid>
    Logfile: <path>

After the email is sent, the method calls die($extra).
If dying is not what you want, this should be done in an eval {}.

This method is partially deprecated.

=cut

sub email_and_die {
  my ($self, $extra) = @_;
  $self->e("Mail sending not available. Install Mail::Send, or perl-MailTools on CentOS") and die("Cannot mail out") unless($mail_available);
  $self->failure_email($extra);
  die($extra);
}


sub failure_email {
  my ($self,$extra) = shift;
  $self->send_email("$self->{script_name} FAILED", $extra);
}

sub success_email {
  my ($self, $extra) = shift;

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

=pod

=head1 ENVIRONMENT

This package responds to the environment variable: C<PDB_DEBUG>.
When C<PDB_DEBUG> is set to something that evaluates as true in perl,
then debugging messages (such as those generated by L<d()>) will be
generated.

=cut

1;
