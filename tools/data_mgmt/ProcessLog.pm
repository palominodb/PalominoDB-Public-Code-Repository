package ProcessLog;
use Mail::Send;
use Sys::Hostname;
use Sys::Syslog qw(:standard :macros);
use Digest::SHA1;
use Time::HiRes qw(time);

use constant _PdbDEBUG => $ENV{Pdb_DEBUG} || 0;
use constant Level1 => 1;
use constant Level2 => 2;
use constant Level3 => 3;

# Creates a new processlog.
# parameters: $script_name, $logpath, $email_to.
#
# $script_name: the name of the program using this. Normally $0.
# $logpath: filename of log. Normally $0.
#           If it matches syslog:<facility>, then output is sent to syslog.
sub new {
  my $class = shift;
  my ($script_name, $logpath, $email_to) = @_;
  my $self = {};

  $self->{run_id} = Digest::SHA1::sha1_hex(time . rand() . $script_name);

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
      my $lvl = LOG_DEBUG;
      $lvl = LOG_INFO if($_[0] eq "msg");
      $lvl = LOG_INFO if($_[0] eq "ifo");
      $lvl = LOG_ERR  if($_[0] eq "err");
      foreach my $l (split "\n", _p(@_)) {
        syslog($lvl, $l);
      }
      print _p(@_) unless $self->{quiet};
    };
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

# Returns script name.
sub name {
  my $self = shift;
  $self->{script_name};
}

# Returns the runid of this processlog.
# If you want a new runid - make a new processlog.
sub runid {
  my $self = shift;
  $self->{run_id};
}

# Logs a 'start' message.
# Programs can use this method, and end to place
# unique ids into the logfile to assist in later processing.
sub start {
  my $self = shift;
  $self->m("BEGIN $self->{run_id}");
}

# Logs an 'end' message. See start().
sub end {
  my $self = shift;
  $self->m("END $self->{run_id}");
}

# Gets/Sets the maximum depth for stack traces.
sub stack_depth {
  my ($self, $opts) = @_;
  my $old = $self->{stack_depth};
  $self->{stack_depth} = $opts if( defined $opts );
  $old;
}

# Gets/Sets whether or not this processlog will log to stdout.
sub quiet {
  my ($self, $new) = @_;
  my $old = $self->{quiet};
  $self->{quiet} = $new if( defined $new );
  $old;
}

# Log a message.
sub m {
  my ($self,$m) = shift;
  my $fh = $self->{LOG};
  my $t = time();
  $self->{logsub}->($self, 'msg', undef, undef, $t, @_);
}

# Log a message with a stack trace.
sub ms {
  my $self = shift;
  $self->m(@_);
  $self->m($self->stack());
}

# Log an error.
sub e {
  my ($self,$m) = shift;
  my ($package, undef, $line) = caller 0;
  my $fh = $self->{LOG};
  my $t = time();
  $self->{logsub}->($self, 'err', $package, $line, $t, @_);
}

# Log an error with a stack trace.
sub es {
  my $self = shift;
  $self->e(@_);
  $self->e($self->stack());
}

# Log some information.
sub i {
  my $self = shift;
  my $fh = $self->{LOG};
  my $t = time();
  $self->{logsub}->($self, 'ifo', undef, undef, $t, @_);
}

# Log some information with a stack trace.
sub is {
  my $self = shift;
  $self->i(@_);
  $self->i($self->stack());
}

# Log debugging output.
sub d {
  my $self = shift;
  my ($package, undef, $line) = caller 0;
  my $fh = $self->{LOG};
  if(_PdbDEBUG) {
    my $t = time();
    $self->{logsub}->($self, 'dbg', $package, $line, $t, @_);
  }
}

# Log debugging output with a stack trace.
sub ds {
  my $self = shift;
  $self->d(@_);
  $self->d($self->stack());
}

# Return a nicely formatted stacktrace.
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

# Send an email to the pre-defined location.
# If no email was specified at creation time, this method does nothing.
# Accepts an argument $extra which is intended to be a summary of the problem.
#
# The format of the email is as follows:
#     Subject: <script_name> FAILED
#
#     <script_name> on <hostname> failed at <time>.
#     The Error: <extra>
#     <stack trace>
#     RUN ID (for grep): <runid>
#     Logfile: <path>
#
# After the email is sent, the method calls die($extra).
# If dying is not what you want, this should be done in an eval {}.
#
# This method is partially deprecated.
sub email_and_die {
  my ($self,$extra) = shift;
  $self->i("Not emailing: $extra") if(not defined $self->{email_to});
  $self->msg("Emailing out failure w/ extra: $extra\n");
  my $msg = Mail::Send->new(Subject => "$self->{script_name} FAILED", To => $self->{email_to});
  my $fh = $msg->open;
  print $fh "$self->{script_name} on ". hostname() . " failed at ". scalar localtime() ."\n";
  print $fh "\nThe Error: $extra\n";
  print $fh $self->stack() . "\n";
  print $fh "RUN ID (for grep): $self->{run_id}\n";
  print $fh "Logfile: $self->{log_path}\n";
  $fh->close;
  die($extra)
}

1;
