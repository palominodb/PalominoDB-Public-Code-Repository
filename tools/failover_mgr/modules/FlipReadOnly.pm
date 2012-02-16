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
