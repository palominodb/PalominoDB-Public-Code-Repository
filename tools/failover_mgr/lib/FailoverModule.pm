# FailoverModule.pm
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

package FailoverModule;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Carp;
use DSN;

our ($pretend, $force);

sub new {
  my ($class, $pri_dsn, $fail_dsn, $opts) = @_;
  $opts ||= {};
  my $self = bless $opts, $class;
  $self->{'primary_dsn'} = $pri_dsn;
  $self->{'failover_dsn'} = $fail_dsn;

  $::PLOG->d('Instantiating:', $self);
  return $self;
}

sub options { return () }

# Sets package variables corresponding to
# global options in FailoverManager package
sub global_opts {
  my $class = shift;
  ($pretend, $force) = @_;
}

sub DESTROY {
  my $self = shift;
  $::PLOG->d('Destroying:', $self);
}

sub run {
  croak("Cannot run FailoverModule base");
}

1;
