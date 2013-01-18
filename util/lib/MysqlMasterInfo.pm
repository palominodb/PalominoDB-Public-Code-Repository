# MysqlMasterInfo.pm - Fore reading and generating a MySQL master.info file.
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
