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
