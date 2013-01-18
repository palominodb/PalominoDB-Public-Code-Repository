# ReplicationLog.pm
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
