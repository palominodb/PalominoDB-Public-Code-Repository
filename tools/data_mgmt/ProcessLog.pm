package ProcessLog;
use Mail::Send;
use Sys::Hostname;
use Digest::SHA1;
use Time::HiRes qw(time);

use constant _PdbDEBUG => $ENV{Pdb_DEBUG} || 0;
use constant Level1 => 1;
use constant Level2 => 2;
use constant Level3 => 3;

sub new {
  my $class = shift;
  my ($script_name, $logpath, $email_to) = @_;
  my $self = {};

  $self->{run_id} = Digest::SHA1::sha1_hex(time . rand() . $script_name);

  $self->{script_name} = $script_name;
  $self->{log_path} = $logpath;
  $self->{email_to} = $email_to;
  $self->{stack_depth} = 10; # Show traces 10 levels deep.
  open $self->{LOG}, ">>$self->{log_path}" or die("Unable to open logfile: '$self->{log_path}'.\n");

  bless $self,$class;
  return $self;
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

sub m {
  my ($self,$m) = shift;
  my $fh = $self->{LOG};
  my $t = time();
  print $fh _p('msg', undef, undef, $t, @_);
  print _p('msg', undef, undef, $t, @_);
}

sub ms {
  my $self = shift;
  $self->m(@_);
  $self->m($self->stack());
}

sub e {
  my ($self,$m) = shift;
  my ($package, undef, $line) = caller 0;
  my $fh = $self->{LOG};
  my $t = time();
  print $fh _p('err', $package, $line, $t, @_);
  print _p('err', $package, $line, $t, @_);
}

sub es {
  my $self = shift;
  $self->e(@_);
  $self->e($self->stack());
}

sub i {
  my $self = shift;
  my $fh = $self->{LOG};
  my $t = time();
  print $fh _p('ifo', undef, undef, $t, @_);
  print _p('ifo', undef, undef, $t, @_);
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
  chomp($m);
  if(_PdbDEBUG) {
    print $fh _p('dbg', $package, $line, time(), @_);
    print STDERR _p('dbg', $package, $line, time(), @_);
  }
}

sub ds {
  my $self = shift;
  $self->d(@_);
  $self->d($self->stack());
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
  my $prefix = "$mode ";
  $prefix .= "${package}:${line} " if(defined $package and defined $line);
  $prefix .= "$time: ";
  @_ = map { (my $temp = $_) =~ s/\n/\n$prefix/g; $temp; }
       map { defined $_ ? $_ : 'undef' } @_;
  $prefix. join(' ',@_). "\n";
}

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
