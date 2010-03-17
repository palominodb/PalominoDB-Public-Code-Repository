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
  #R_print('ACK');
  return @recv;
}

use constant CODE => '__CODE__';
use constant CODE_DIGEST => '__SHA1__';
$0 = "Remote perl object from ". ($ENV{'SSH_CLIENT'} || 'localhost');

R_die(TRANSPORT_FAILURE, "Code digest does not match") unless(sha1_hex(CODE) eq CODE_DIGEST);

no warnings 'once';
# This is set to cause the eval to occur in our namespace.
# This prevents autoloader errors originating in Storable.pm
local $Storable::Eval = sub {
  # Cheap hack to remove package inserted by B::Deparse
  # This obviously will break any attempt to Actually
  # Insert package specific subs.
  $_[0] =~ s/package.*;$//m;
  if($ENV{'ROBJ_LOCAL_DEBUG'}) { print $_[0]; }
  my $r = eval "$_[0]";
  R_die(COMPILE_FAILURE, "Unable to compile transported subroutine eval: $@") if($@);
  return $r;
};
my $code = thaw(decode_base64(CODE));
use warnings FATAL => 'all';

# use strict normally prevents doing
# tricky (and usually unintended) typeglob and symbol table
# manipulation. Turning off strict refs allows that.
# We do it so that subs defined remotely appear in the symbol table
# as 'real' subroutines that needn't be called with dereferencing
# or other annoying things.
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
