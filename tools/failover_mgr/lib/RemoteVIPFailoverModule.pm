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