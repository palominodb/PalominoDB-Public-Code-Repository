# ReadOnly.pm
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
