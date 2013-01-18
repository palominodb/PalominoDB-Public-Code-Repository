# E.pm
# Copyright (C) 2009-2013 PalominoDB, Inc.
# 
# You may contact the maintainers at eng@palominodb.com.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

package RObj::Base;
use strict;
use warnings FATAL => 'all';
use 5.0008;
use English qw(-no_match_vars);
use Storable qw(thaw nfreeze);
use MIME::Base64;
use Carp;

use Data::Dumper;

use Exporter;
use vars qw(@ISA $VERSION @EXPORT);

@ISA = qw(Exporter);
@EXPORT = qw(COMPILE_FAILURE TRANSPORT_FAILURE OK);

$VERSION = 0.01;

use constant NATIVE_DEATH      => -3;
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
    my $b64 = "";
    for (@lines) {
      ROBJ_NET_DEBUG >=2 && print STDERR "recv: parsing: $_\n";
      if(/^ok$/) {
        ROBJ_NET_DEBUG >=2 && print STDERR "recv: found complete object\n";
        eval {
          push @res, @{thaw(decode_base64($b64))};
        };
        if($EVAL_ERROR) {
          push @res, ['INVALID MESSAGE', $EVAL_ERROR, "${b64}\n"];
        }
        $b64 = "";
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
  my $buf;
  eval {
    $buf = encode_base64(nfreeze(\@objs));
  };
  if($EVAL_ERROR) {
    croak $EVAL_ERROR;
  }
  $self->{Sys_Error} = 0;
  ROBJ_NET_DEBUG && print STDERR "send(". length($buf) ."b): ${buf}ok\n";
  return syswrite($fh, $buf ."ok\n");
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
# This BEGIN block exists to catch errors during
# the setup of the remote object. Namely, importing
# the necessary modules.
# It turns the STDERR messages from 'use' failures
# into messages back to the controlling process.
BEGIN {
  $SIG{__DIE__} = sub {
    die @_ if $^S;
    my $ro = RObj::Base->new;
    $ro->write_message(\*STDOUT, @_);
    exit(RObj::Base::COMPILE_FAILURE);
  };
}
use Storable qw(nfreeze thaw);
use MIME::Base64;
use IO::Handle;
use English qw(-no_match_vars);

RObj::Base->import;

use constant COMPILE_FAILURE => RObj::Base::COMPILE_FAILURE;
use constant TRANSPORT_FAILURE => RObj::Base::TRANSPORT_FAILURE;
use constant NATIVE_DEATH => RObj::Base::NATIVE_DEATH;
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
  exit(OK);
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
$0 = "Remote perl object from ". ($ENV{'SSH_CLIENT'} || 'localhost');

my $code = thaw(decode_base64(CODE));

# use strict normally prevents doing
# tricky (and usually unintended) typeglob and symbol table
# manipulation. Turning off strict refs allows that.
# We do it so that subs defined remotely appear in the symbol table
# as 'real' subroutines that needn't be called with dereferencing
# or other annoying things.
{
  no strict 'refs';
  foreach my $cr (@{$code}) {
    my $name = $cr->[0];
    if($name =~ /^_use_/ ) {
      &{eval "sub $cr->[1]"}();
      if($@) {
        R_die(COMPILE_FAILURE, "Unable to use ($name). eval: $@");
      }
      next;
    }
    if($name =~ /::BEGIN/) {
      eval "$name $cr->[1]";
      if($@) {
        R_die(COMPILE_FAILURE, "Unable to compile transported BEGIN ($name). eval: $@");
      }
      next;
    }
    my $subref = eval "sub $cr->[1];";
    if($@) {
      R_die(COMPILE_FAILURE, "Unable to compile transported sub ($name). eval: $@");
    }
    *{$name} = $subref;
  }
}

$| = 1;

R_print('READY');
my @args = R_read();
R_print('ACK');
$SIG{__DIE__} = sub { die @_ if $^S; R_die(NATIVE_DEATH, @_); };
R_exit(
  R_main(
    @args
  )
);

1;
