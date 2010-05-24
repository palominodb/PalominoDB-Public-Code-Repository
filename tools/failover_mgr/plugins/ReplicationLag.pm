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
package ReplicationLag;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Exporter;
use Carp;
use DBI;
use FailoverPlugin;
our @ISA = qw(FailoverPlugin);

sub options {
  return ( 'hb_table=s', 'hb_col=s' );
}

sub get_lag {
  my $self = shift;
  my $dsn = shift;
  my $sql = 'SHOW SLAVE STATUS';
  my $col = 'Seconds_Behind_Master';

  if($self->{'hb_table'} and $self->{'hb_col'}) {
    my $hb_table = $self->{'hb_table'};
    my $hb_col = $self->{'hb_col'} || 'ts';
    $::PLOG->d('Using heartbeat table:', $self->{'heartbeat'});
    $sql = "SELECT NOW() - $hb_col FROM $hb_table";
    $col = $hb_col;
  }
  my $r = $dsn->get_dbh(1)->selectrow_hashref($sql, { Slice => {} });
  if(defined $r) {
    return $r->{$col};
  }
  return undef;
}

sub pre_verification {
  my ($self,@dsns) = @_;

  foreach my $dsn (@dsns) {
    my $lag = $self->get_lag($dsn);
    if(not defined($lag)) {
      $::PLOG->e('No replication, or replication not running.');
      croak('No replication, or replication not running') unless($FailoverPlugin::force)
    }
    $::PLOG->m($dsn->get('h'),'replication lag:', $lag);
    if($lag) { $::PLOG->e('Replication lag found!'); }
    if($lag and !$FailoverPlugin::force) {
      croak('Replication lag');
    }
    elsif($lag and $FailoverPlugin::force) {
      my $r = $::PLOG->p('Continue with lag [Yes/no]?',
        qr/^(Yes|No)$/i, 'Yes');
      if(lc($r) eq 'no') {
        croak('Replication lag');
      }
    }
  }

}

sub post_verification {
  my ($self, $status, @dsns) = @_;
  foreach my $dsn (@dsns) {
    my $lag = $self->get_lag($dsn);
    $::PLOG->m($dsn->get('h'), 'replication lag:', $lag);
  }
}

1;
