# FlipReadOnly.pm
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
 
package FlipReadOnly;
use strict;
use warnings FATAL => 'all';
use Carp;
use DBI;
use ProcessLog;
use DSN;
use FailoverPlugin;
use FailoverModule;
our @ISA = qw(FailoverModule);

our $pretend;

sub run {
  my ($self) = @_;
  my $pdsn = $self->{'primary_dsn'};
  my $fdsn = $self->{'failover_dsn'};
  my $fdbh;
  my $status = 1;
  FailoverPlugin->pre_verification_hook($pdsn, $fdsn);
  FailoverPlugin->begin_failover_hook($pdsn, $fdsn);

  $::PLOG->m('Connecting to failover master.');
  eval {
    $fdbh = $fdsn->get_dbh(1);
  };
  if($@) {
    $::PLOG->e('Failover FAILED.');
    $::PLOG->e('DBI threw:', $@);
    FailoverPlugin->finish_failover_hook(0, $pdsn, $fdsn);
    FailoverPlugin->post_verification_hook(0, $pdsn, $fdsn);
    croak($@);
  }

  $::PLOG->m('Setting read_only=0');

  eval {
    $fdbh->do('SET GLOBAL read_only=0') unless($FailoverModule::pretend);
  };
  if($@) {
    $::PLOG->e('Failover FAILED.');
    $::PLOG->e('DBI threw:', $@);
    FailoverPlugin->finish_failover_hook(0, $pdsn, $fdsn);
    FailoverPlugin->post_verification_hook(0, $pdsn, $fdsn);
    croak($@);
  }

  FailoverPlugin->finish_failover_hook(1, $pdsn, $fdsn);
  FailoverPlugin->post_verification_hook(1, $pdsn, $fdsn);

  return 0;
}

1;
