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
package ReadOnly;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Exporter;
use MysqlSlave;
use Carp;
use MysqlSlave;
use FailoverPlugin;
our @ISA = qw(FailoverPlugin);

sub pre_verification {
  my ($self, $pdsn, $fdsn) = @_;
  my $fslave = MysqlSlave->new($fdsn);
  $self->{read_only_var} = $fslave->read_only();
  if(!$self->{read_only_var}) {
    $::PLOG->i('Warning: read_only is NOT set to 1 on', $fdsn->get('h'));
    if(!$FailoverPlugin::force) {
      my $r = $::PLOG->p('Continue failover [Yes/No]:', qr/^(Yes|No)$/i); 
      if(lc($r) eq lc('No')) {
        croak('Aborting failover'); 
      }
    }
    else {
      $::PLOG->i('Warning: --force used, ignoring failure.');
    }
  }
  else {
    $::PLOG->m('read_only is set to 1 on', $fdsn->get('h'));
  }
}

sub post_verification {
  my ($self, $status, $pdsn, $fdsn) = @_;
  my $fslave = MysqlSlave->new($fdsn);
  if($self->{read_only_var} == $fslave->read_only()) {
    $::PLOG->i('Warning: read_only was not switched on', $fdsn->get('h'));
  }
  else {
    $::PLOG->m('read_only switched.');
  }
}

1;
