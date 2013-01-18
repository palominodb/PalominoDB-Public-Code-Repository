# VIPFailoverModule.pm
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


package VIPFailoverModule;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Exporter;
use Carp;
use DSN;
use FailoverModule;
our @ISA = qw(FailoverModule);

sub new {
  my ($class, $pri_dsn, $fail_dsn, $opts) = @_;
  $$opts{'vip-timeout'} ||= 300; # Default to 5 minute VIP move timeout
  if(not exists $$opts{'vip'}) {
    die("--vip option required for VIP modules");
  }
  return bless $class->SUPER::new($pri_dsn, $fail_dsn, $opts), $class;
}

sub options {
  return ('vip=s', 'vip-timeout=i');
}

# ##########################################################################
# VIP plugin hooks
# ----------------
# Implement these in subclasses for different kinds of VIP failover
# For example, one implementation could call down to crm_resource from
# corosync/heartbeat/etc and another could call out to a webservice
#
# All of these methods are passed:
# - a $self reference
# - a DSN object describing the real host
# - the name of a VIP to manipulate
# Every single one of these methods MUST croak() if there is an error.
# ##########################################################################

## This sub is called during new() so that the plugin can do any onetime
## operations needed. This function is only passed a reference to $self.
## It MUST NOT manipulate the state of the VIP, since this method
## will be called after pre-verification checks but before begin-failover.
sub init {
  die("Cannot use VIPFailoverModule base");
}

## This sub is used to ensure that the VIP is in a consistent state
## prior to failover. This should be used for permissions checking,
## software status, etc. It should die() if it finds an error.
## This method is called twice. Once for the primary DSN and once
## for the failover DSN.
## It should not depend on the order in which it is called, nor should
## it care what host it is called with. The purpose of this method
## is to verify that the stateless configuration of the host is correct.
## For example:
##   Given a VIP implementation that uses a state file to determine
##   VIP location.
##   This method would check that the necessary directories exist, and
##   that the appropriate user has permissions to create and/or modify files.
##   It might also ensure that the VIP software is running.
sub verify_vip {
  die("Cannot use VIPFailoverModule base");
}

## This sub is used to wait for the VIP to become present on a host
## In addition to the standard parameters, this sub also is passed
## a $timeout value, which is the time in seconds to wait.
## Implementations SHOULD NOT ignore the timeout value.
## This method is also used to check if a VIP is present, so,
## an implementation MAY return immediately if it can determine
## that the VIP will never become present.
sub wait_vip {
  die("Cannot use VIPFailoverModule base");
}

## This sub is used to add a VIP to a host.
sub add_vip {
  die("Cannot use VIPFailoverModule base");
}

## This sub is used to remove a VIP from a host.
sub del_vip {
  die("Cannot use VIPFailoverModule base");
}

# ##########################################################################
# End of VIP plugin hooks
# ##########################################################################

sub run {
  my ($self) = @_;
  my $pdsn = $$self{'primary_dsn'};
  my $fdsn = $$self{'failover_dsn'};
  my $vip  = $$self{'vip'};
  FailoverPlugin->pre_verification_hook($pdsn, $fdsn);
  $self->init();
  eval{
    $self->verify_vip($pdsn, $vip);
    $self->verify_vip($fdsn, $vip);
  };
  if($@) {
    $::PLOG->e($@);
    return 1;
  }
  FailoverPlugin->begin_failover_hook($pdsn, $fdsn);
  eval {
    $self->wait_vip($pdsn, $vip, 0); ## Check that the VIP exists on the primary
    $self->del_vip($pdsn, $vip);   ## Remove the VIP from the primary
    $self->add_vip($fdsn, $vip);   ## Add the VIP to the secondary
    $self->wait_vip($fdsn, $vip, $$self{'timeout'});  ## Wait for VIP to exist
  };
  if($@) {
    $::PLOG->e($@);
    FailoverPlugin->finish_failover_hook(0, $$self{'primary_dsn'}, $$self{'failover_dsn'});
    FailoverPlugin->post_verification_hook(0, $$self{'primary_dsn'}, $$self{'failover_dsn'});
    return 1;
  }
  FailoverPlugin->finish_failover_hook(1, $$self{'primary_dsn'}, $$self{'failover_dsn'});
  FailoverPlugin->post_verification_hook(1, $$self{'primary_dsn'}, $$self{'failover_dsn'});
}
