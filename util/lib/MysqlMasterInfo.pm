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
package MysqlMasterInfo;
use strict;
use warnings FATAL => 'all';

sub open {
  my ($class, $path) = @_;
  my $self = {};
  my $fh;
  $self->{path} = $path;
  CORE::open($fh, '<', $path) or return $!;
  $self->{lines} = ();
  chomp(@{$self->{lines}} = <$fh>);
  close($fh);

  return bless $self, $class;
}

sub write {
  my ($self, $path) = @_;
  my $write_path = ($path || $self->{path});
  my $fh;
  CORE::open($fh, '>', $write_path) or return undef; 
  {
    local $, = "\n";
    print $fh @{$self->{lines}};
  }
  close($fh);
  return 0;
}

sub log_file {
  my $self = shift;
  return $self->_update(1, qr/[^\0]+/, @_);
}

sub log_pos {
  my $self = shift;
  return $self->_update(2, qr/\d+/, @_);
}

sub master_host {
  my $self = shift;
  return $self->_update(3, qr/.+/, @_);
}

sub master_user {
  my $self = shift;
  return $self->_update(4, qr/.+/, @_);
}

sub master_password {
  my $self = shift;
  return $self->_update(5, qr/.*/, @_);
}

sub master_port {
  my $self = shift;
  return $self->_update(6, qr/\d+/, @_);
}

sub connect_retry {
  my $self = shift;
  return $self->_update(7, qr/\d+/, @_);
}

sub master_ssl_allowed {
  my $self = shift;
  return $self->_update(8, qr/0|1/, @_);
}

sub master_ssl_ca_file {
  my $self = shift;
  return $self->_update(9, qr/[^\0]*/, @_);
}

sub master_ssl_ca_path {
  my $self = shift;
  return $self->_update(10, qr/[^\0]*/, @_);
}

sub master_ssl_cert {
  my $self = shift;
  return $self->_update(11, qr/[^\0]*/, @_);
}

sub master_ssl_cipher {
  my $self = shift;
  return $self->_update(12, qr/[\w\-_]*/, @_);
}

sub master_ssl_key {
  my $self = shift;
  return $self->_update(13, qr/[^\0]*/, @_);
}

sub master_ssl_verify_server_cert {
  my $self = shift;
  return $self->_update(14, qr/0|1/, @_);
}

sub _update {
  my ($self, $lineno, $filter, $new) = @_;
  my $old = $self->{lines}->[$lineno];
  if(defined($new) and $new =~ $filter) {
    $self->{lines}->[$lineno] = ($new || $old);
  }
  return $old;
}

1;

=pod

=head1 NAME

MysqlMasterInfo - Represents a master.info file

=head1 SYNOPSIS

This package is for reading and generating a mysql C<master.info> file.
It's assumed you are familiar with the file and the terms.

=head1 (DE)CONSTRUCTION

=over 8

=item open($path)

Takes a C<$path> to a C<master.info> file and returns a new C<MysqlMasterInfo> object.

=item write([$path])

If given a C<$path>, write out a C<master.info> to the given C<$path>.
Otherwise, write out to the path the object was opened with.

This method B<WILL> let you shoot yourself in the foot. You have been warned.

=back

=head1 INSTANCE METHODS

Each method takes either zero or one parameters.
If given a parameter it sets the corresponding column to the value of that parameter and returns the old value. Otherwise, returns the current value.

=over 8

=item log_file([$new])

=item log_pos([$new])

=item master_host([$new])

=item master_user([$new])

=item master_password([$new])

=item master_port([$new])

=item connect_retry([$new])

=item master_ssl_allowed([$new])

=item master_ssl_ca_file([$new])

=item master_ssl_ca_path([$new])

=item master_ssl_cert([$new])

=item master_ssl_cipher([$new])

=item master_ssl_key([$new])

=back

=head1 REFERENCES

File format for C<master.info>:

L<http://dev.mysql.com/doc/refman/5.0/en/slave-logs-status.html>

=cut
