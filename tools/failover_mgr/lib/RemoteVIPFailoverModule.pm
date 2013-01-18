# RemoteVIPFailoverModule.pm
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
 
package RemoteVIPFailoverModule;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Exporter;
use Carp;
use DSN;
use VIPFailoverModule;
our @ISA = qw(VIPFailoverModule);

# ##########################################################################
# Failover modules that inherit from this class have their hooks executed
# on the remote end.
# ##########################################################################

sub new {
  my ($class, $pri_dsn, $fail_dsn, $opts) = @_;
  croak('Required key sU not present in primary DSN') unless $pri_dsn->has('sU');
  croak('Required key sU not present in secondary DSN') unless $fail_dsn->has('sU');
  return bless $class->SUPER::new($pri_dsn, $fail_dsn, $opts), $class;
}

## Wraps running an RObj which mutates $self
## Implied usage is: $self = $self->_run($ro, @args);
## It'll croak if the status returned by the RObj is not 'EXIT'
sub _run {
  my ($self, $ro, @args) = @_;
  my ($status, $res) = $ro->do($self, @args);
  if($status ne 'EXIT') {
    croak($status);
  }
  return $res;
}

sub _chld_sub {
  no strict 'refs';
  my ($self, $sub) = @_;
  return \&{ref($self).'::'.$sub};
}

sub run {
  my ($self) = @_;
  my $pdsn = $$self{'primary_dsn'};
  my $fdsn = $$self{'failover_dsn'};
  my $vip  = $$self{'vip'};

  my $check_vip = RObj->new($pdsn);
  $check_vip->add_package('DSN');
  $check_vip->add_main($self->_chld_sub('wait_vip'));

  my $pverify_vip = RObj->new($pdsn);
  $pverify_vip->add_package('DSN');
  $pverify_vip->add_main($self->_chld_sub('verify_vip'));

  my $fverify_vip = RObj->new($fdsn);
  $fverify_vip->add_package('DSN');
  $fverify_vip->add_main($self->_chld_sub('verify_vip'));

  my $del_vip = RObj->new($pdsn);
  $del_vip->add_package('DSN');
  $del_vip->add_main($self->_chld_sub('del_vip'));

  my $add_vip = RObj->new($fdsn);
  $add_vip->add_package('DSN');
  $add_vip->add_main($self->_chld_sub('add_vip'));

  my $wait_vip = RObj->new($fdsn);
  $wait_vip->add_package('DSN');
  $wait_vip->add_main($self->_chld_sub('wait_vip'));

  FailoverPlugin->pre_verification_hook($pdsn, $fdsn);
  $self->init();
  eval {
    $::PLOG->d('Running verify_vip: $pdsn');
    $self = $self->_run($pverify_vip, $pdsn, $vip);
    $::PLOG->d('Running verify_vip: $fdsn');
    $self = $self->_run($fverify_vip, $fdsn, $vip);
  };
  if($@) {
    $::PLOG->e($@);
    return 1;
  }
  FailoverPlugin->begin_failover_hook($$self{'primary_dsn'}, $$self{'failover_dsn'});
  eval {
    $::PLOG->d('Running check_vip $pdsn');
    $self = $self->_run($check_vip, $pdsn, $vip, 0);
    $::PLOG->d('Running del_vip $pdsn');
    $self = $self->_run($del_vip, $pdsn, $vip);
    $::PLOG->d('Running add_vip $fdsn');
    $self = $self->_run($add_vip, $fdsn, $vip);
    $::PLOG->d('Running wait_vip $fdsn');
    $self = $self->_run($wait_vip, $fdsn, $vip, $$self{'vip-timeout'});
  };
  if($@) {
    $::PLOG->e($@);
    FailoverPlugin->finish_failover_hook(0, $$self{'primary_dsn'}, $$self{'failover_dsn'});
    FailoverPlugin->post_verification_hook(0, $$self{'primary_dsn'}, $$self{'failover_dsn'});
    return 1;
  }
  FailoverPlugin->finish_failover_hook(1, $$self{'primary_dsn'}, $$self{'failover_dsn'});
  FailoverPlugin->post_verification_hook(1, $$self{'primary_dsn'}, $$self{'failover_dsn'});
  return 0;
}
