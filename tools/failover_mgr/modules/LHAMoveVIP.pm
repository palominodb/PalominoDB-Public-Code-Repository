# LHAMoveVIP.pm
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
 
package LHAMoveVIP;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Exporter;
use RObj;
use Carp;
use RemoteVIPFailoverModule;
our @ISA = qw(RemoteVIPFailoverModule);

# empty init
sub init { }

sub verify_vip {
  require XML::Simple;
  my ( $self, $dsn, $vip ) = @_;
  my $crm_mon_out;
  my $host;
  chomp( $host           = qx/uname -n/ );
  chomp( $$self{crm_mon} = qx/which crm_mon/ );
  chomp( $$self{crm}     = qx/which crm/ );
  die("Could not find crm_mon") unless ( $$self{crm_mon} );
  die("Could not find crm")     unless ( $$self{crm} );
  ## load cluster information from XML
  my $cib = XML::Simple::XMLin( join( "\n", qx/crm configure show xml/ ) );
  my $vipcfg;
  $$self{cib} = $cib;
  ## CIB is slightly different when there are multiple groups
  if ( exists $cib->{'configuration'}->{'resources'}->{'group'}->{$vip} ) {
    $vipcfg = $cib->{'configuration'}->{'resources'}->{'group'}->{$vip};
  }
  elsif ( $cib->{'configuration'}->{'resources'}->{'group'}->{'id'} eq $vip ) {
    $vipcfg = $cib->{'configuration'}->{'resources'}->{'group'};
  }
  else {
    croak("Unknown vip $vip");
  }

  chomp( $crm_mon_out = qx/$$self{crm_mon} -1/ );
  die("Host does not appear to be in cluster")
    unless ( $crm_mon_out =~ /$host/m );
  return $self;
}

sub wait_vip {
  my ( $self, $dsn, $vip, $timeout ) = @_;
  my $cib = $$self{cib};
  my $vipcfg;
  my @ips;
  my $t = 0;
  ## CIB is slightly different when there are multiple groups
  if ( exists $cib->{'configuration'}->{'resources'}->{'group'}->{$vip} ) {
    $vipcfg = $cib->{'configuration'}->{'resources'}->{'group'}->{$vip};
  }
  elsif ( $cib->{'configuration'}->{'resources'}->{'group'}->{'id'} eq $vip ) {
    $vipcfg = $cib->{'configuration'}->{'resources'}->{'group'};
  }
  else {
    die("Unknown vip $vip");
  }
  if(not exists $$self{'primary_dsn'}->{'vI'}) {
    my %tmp;
    foreach my $res (keys %{$vipcfg->{'primitive'}}) {
      if(exists $vipcfg->{'primitive'}->{$res}->{'instance_attributes'}->{'nvpair'}->{'ip'}) {
        $tmp{$vipcfg->{'primitive'}->{$res}->{'instance_attributes'}->{'nvpair'}->{'ip'}->{'value'}} = 1;
      }
    }
    $$self{'primary_dsn'}->{'vI'}->{'value'} = [sort keys %tmp];
    $$self{'failover_dsn'}->{'vI'}->{'value'} = [sort keys %tmp]
  }
  @ips = @{$$self{'primary_dsn'}->{'vI'}->{'value'}};
  foreach my $ip (@ips) {
    while(system("ifconfig | grep -q $ip") >> 8) {
      sleep 1;
      if($t++ > $timeout) {
        die('VIP not up; needed: ['. join(',', @ips) .']');
      }
    }
  }
  
  return $self;
}

sub add_vip {
  my ( $self, $dsn, $vip ) = @_;
  my $out = qx/$$self{crm} resource move $vip `uname -n`/;
  die("Error moving $vip: ". ($out ? $out : '(no output)')) if($?>>8);
  return $self;
}

sub del_vip {
  my ( $self, $dsn, $vip ) = @_;
  return $self;
}

1;
