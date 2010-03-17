# ###########################################################################
# RObj::Base package f4c85adff1164b4360db91361e47429f47deabfc
# ###########################################################################
package RObj::Base;
use strict;
use warnings FATAL => 'all';
use 5.0008;
use English qw(-no_match_vars);
use Storable qw(thaw freeze);
use MIME::Base64;
use Digest::SHA qw(sha1_hex);

use Data::Dumper;

use Exporter;
use vars qw(@ISA $VERSION @EXPORT);

@ISA = qw(Exporter);
@EXPORT = qw(COMPILE_FAILURE TRANSPORT_FAILURE OK);

$VERSION = 0.01;

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
    my ($b64, $sha1) = ("", "");
    for (@lines) {
      ROBJ_NET_DEBUG >=2 && print STDERR "recv: parsing: $_\n";
      if(/^ok$/ and sha1_hex($b64) eq $sha1) {
        ROBJ_NET_DEBUG >=2 && print STDERR "recv: found complete object\n";
        eval {
          push @res, @{thaw(decode_base64($b64))};
        };
        if($EVAL_ERROR) {
          push @res, ['INVALID MESSAGE', "${b64}$sha1\n"];
        }
        $b64 = "";
        $sha1 = "";
      }
      elsif(/^ok$/ and sha1_hex($b64) ne $sha1) {
        ROBJ_NET_DEBUG >=2 && print STDERR "recv: found invalid object\n";
        push @res, ['INVALID OBJECT', "${b64}$sha1\n"];
        $b64 = "";
        $sha1 = "";
      }
      elsif(/^[a-f0-9]{40}$/) {
        $sha1 = $_ if($sha1 eq "");
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
  my $buf = encode_base64(freeze(\@objs));
  $buf .= sha1_hex($buf);
  $self->{Sys_Error} = 0;
  ROBJ_NET_DEBUG && print STDERR "send(". length($buf) ."b): $buf\nok\n";
  return syswrite($fh, $buf ."\nok\n");
}

sub sys_error {
  my ($self) = @_;
  return  $self->{Sys_Error};
}

1;
# ###########################################################################
# End RObj::Base package
# ###########################################################################
package RObj;
use strict;
use warnings;
use 5.008;

use Storable qw(freeze thaw);
use MIME::Base64;
use Digest::SHA qw(sha1_hex);
use IPC::Open3;
use IO::Select;
use IO::Handle;
use POSIX;
use Exporter;
use B::Deparse;
use Carp;

# Bring exported symbols into this package.
RObj::Base->import;

use vars qw(@ISA @EXPORT $VERSION);
$VERSION = 0.01;
@ISA = qw(Exporter RObj::Base);

@EXPORT = qw(R_die R_exit R_read R_write COMPILE_FAILURE TRANSPORT_FAILURE OK);

no warnings 'once';
$Storable::Deparse = 1;
use warnings FATAL => 'all';

sub new {
  my ($class, $host, $user, $ssh_key) = @_;
  my $s = RObj::Base->new;
  bless $s, $class;
  $s->{host} = $host;
  $s->{user} = $user;
  $s->{ssh_key} = $ssh_key;
  $s->{code} = ();
  return $s;
}

# This is the subroutine that will
# Be called as the entry point for your RObj.
sub add_main {
  my ($self, $coderef) = @_;
  die("Not a coderef '$coderef'") unless ref($coderef) eq 'CODE';
  push @{$self->{code}}, ['R_main', $coderef];
}

# Call this for any auxiliary subroutines
# your RObj will need.
sub add_sub {
  my ($self, $name, $coderef) = @_;
  die("Not a coderef '$coderef'") unless ref($coderef) eq 'CODE';
  push @{$self->{code}}, [$name, $coderef];
}

# Blocks until at least one message completely received
sub read {
  my ($self) = @_;
  my @recv;
  1 while( !(@recv = $self->read_message($self->{ssh_ofh})) and $self->sys_error() == 0 );
  #$self->write('ACK');
  return @recv;
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
  $self->read();
}

sub debug {
  my ($self, $to) = @_;
  $self->{debug} = $to;
}

sub start {
  my ($self, @rparams) = @_;
  my $code = $self->_wrap;
  my ($ssh_out, $ssh_err, $ssh_in, $exitv, $out, $err);
  $self->{ssh_pid} = open3($ssh_in, $ssh_out, $ssh_err,
    'ssh', $self->{ssh_key} ? ('-i', $self->{ssh_key}) : (),
    '-l', $self->{user}, $self->{host},
    $self->{debug} ?
      qq(PERLDB_OPTS="RemotePort=$self->{debug}" perl -d)
      : 'perl');

  # Write out the code to the temporary file.
  # The inlined Ctrl-D signals to perl that it's done reading source
  # And now must execute it. It is NOT a typo.
  syswrite($ssh_in, "$code\n\n");
  $ssh_in->flush();

  $self->{ssh_ifh} = $ssh_in;
  $self->{ssh_efh} = $ssh_err;
  $self->{ssh_ofh} = $ssh_out;
  # Wait for remote end to come up.
  my @r = $self->read();
  if($r[0] ne 'READY') {
    croak "Remote end did not come up properly. Expected: 'READY'; Got: @r";
  }
  else {
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
  my $code = encode_base64(freeze($self->{code}));
  my $code_sha = sha1_hex($code);
  my $cnt =<<'EOF';
# ###########################################################################
# RObj::E package 70c7ced464c72997219fdce492f818f955f429d4
# ###########################################################################
package RObj::Base;
use strict;
use warnings FATAL => 'all';
use 5.0008;
use English qw(-no_match_vars);
use Storable qw(thaw freeze);
use MIME::Base64;
use Digest::SHA qw(sha1_hex);

use Data::Dumper;

use Exporter;
use vars qw(@ISA $VERSION @EXPORT);

@ISA = qw(Exporter);
@EXPORT = qw(COMPILE_FAILURE TRANSPORT_FAILURE OK);

$VERSION = 0.01;

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
    my ($b64, $sha1) = ("", "");
    for (@lines) {
      ROBJ_NET_DEBUG >=2 && print STDERR "recv: parsing: $_\n";
      if(/^ok$/ and sha1_hex($b64) eq $sha1) {
        ROBJ_NET_DEBUG >=2 && print STDERR "recv: found complete object\n";
        eval {
          push @res, @{thaw(decode_base64($b64))};
        };
        if($EVAL_ERROR) {
          push @res, ['INVALID MESSAGE', "${b64}$sha1\n"];
        }
        $b64 = "";
        $sha1 = "";
      }
      elsif(/^ok$/ and sha1_hex($b64) ne $sha1) {
        ROBJ_NET_DEBUG >=2 && print STDERR "recv: found invalid object\n";
        push @res, ['INVALID OBJECT', "${b64}$sha1\n"];
        $b64 = "";
        $sha1 = "";
      }
      elsif(/^[a-f0-9]{40}$/) {
        $sha1 = $_ if($sha1 eq "");
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
  my $buf = encode_base64(freeze(\@objs));
  $buf .= sha1_hex($buf);
  $self->{Sys_Error} = 0;
  ROBJ_NET_DEBUG && print STDERR "send(". length($buf) ."b): $buf\nok\n";
  return syswrite($fh, $buf ."\nok\n");
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
use Storable qw(freeze thaw);
use MIME::Base64;
use Digest::SHA qw(sha1_hex);
use IO::Handle;

RObj::Base->import;

use constant COMPILE_FAILURE => RObj::Base::COMPILE_FAILURE;
use constant TRANSPORT_FAILURE => RObj::Base::TRANSPORT_FAILURE;
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
  exit($exit_code);
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
use constant CODE_DIGEST => '__SHA1__';
$0 = "Remote perl object from ". ($ENV{'SSH_CLIENT'} || 'localhost');

R_die(TRANSPORT_FAILURE, "Code digest does not match") unless(sha1_hex(CODE) eq CODE_DIGEST);

no warnings 'once';
local $Storable::Eval = sub {
  $_[0] =~ s/package.*;$//m;
  if($ENV{'ROBJ_LOCAL_DEBUG'}) { print $_[0]; }
  my $r = eval "$_[0]";
  R_die(COMPILE_FAILURE, "Unable to compile transported subroutine eval: $@") if($@);
  return $r;
};
my $code = thaw(decode_base64(CODE));
use warnings FATAL => 'all';

no strict 'refs';
foreach my $cr (@{$code}) {
  my $name = $cr->[0];
  my $subref = $cr->[1];
  *{$name} = $subref;
}
use strict;

$| = 1;

R_print('READY');
my @args = R_read();
R_print('ACK');
R_exit(
  R_main(
    @args
  )
);

1;
# ###########################################################################
# End RObj::E package
# ###########################################################################
EOF
  $cnt =~ s/__CODE__/$code/;
  $cnt =~ s/__SHA1__/$code_sha/;

  $cnt;
}

sub R_read {

}

sub R_write {

}

sub R_die {
  die @_;
}

sub R_exit {
  my $e = shift;
  print Dumper(@_);
  exit($e);
}

sub DESTROY {
  my $s = shift;
  waitpid $s->{ssh_pid}, 0;
}


1;
=pod

=head1 NAME

RObj - Remote perl subroutines over SSH.

=head1 SYNOPSIS

Easy usage:

    my $ro = RObj->new('example.host', 'user');
    $ro->add_main( sub {
        # Apend the return status to the output
        Rprint(system('/etc/init.d/my_service restart'));
        # And the hostname of the remote host
        Rprint(`hostname -f`);
        # And the uptime
        Rprint(`uptime`);
        return 0;
      }
    );

=head1 DESCRIPTION

RObj makes it easy to execute perl code on remote hosts over SSH.

This library differs from other similar implementations in that it makes use of
L<Storable> and L<B::Deparse> to send already-compiled perl subs over the wire.
This has the advantage that syntax checking is done ahead of time. When designed
correctly, subs can even be used locally which simplifies testing.

=head1 METHODS



=cut
