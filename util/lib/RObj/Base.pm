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
package RObj::Base;
use strict;
use warnings FATAL => 'all';
use 5.0008;
use English qw(-no_match_vars);
use Storable qw(thaw freeze);
use MIME::Base64;
use Digest::SHA qw(sha1_hex);
use Carp;

use Data::Dumper;

use Exporter;
use vars qw(@ISA $VERSION @EXPORT);

@ISA = qw(Exporter);
@EXPORT = qw(COMPILE_FAILURE TRANSPORT_FAILURE OK);

# Protocol version AND RObj version
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

# Reads data in from the specified filehandle,
# Stores into buffer until at least one message available
# May return more than one message.
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
        # thaw will croak on invalid data.
        # it would be bad if we died when that happened.
        # For that reason we trap and convert to an 'INVALID MESSAGE' error.
        eval {
          push @res, @{thaw(decode_base64($b64))};
        };
        if($EVAL_ERROR) {
          push @res, ['INVALID MESSAGE', $EVAL_ERROR, "${b64}$sha1\n"];
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
    # Reset the buffer to whatever was left-over
    # So that subsequent calls don't parse old messages.
    $self->{Msg_Buffer} = $b64;
  }
  ROBJ_NET_DEBUG && print STDERR "recv obj: ". Dumper(\@res);
  return @res;
}

# Freezes objects and sends them over the wire.
sub write_message {
  my ($self, $fh, @objs) = @_;
  my $buf;
  eval {
    $buf = encode_base64(freeze(\@objs));
  };
  if($EVAL_ERROR) {
    croak $EVAL_ERROR;
  }
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
