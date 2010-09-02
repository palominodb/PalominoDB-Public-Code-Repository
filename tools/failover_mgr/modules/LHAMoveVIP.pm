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


#our $tcib = [
#  'EXIT',
#  bless(
#    {
#      'failover_dsn' => bless(
#        {
#          'F' =>
#            { 'desc' => 'Defaults File', 'default' => '', 'mandatory' => 0 },
#          'S' => { 'desc' => 'Socket path', 'default' => '', 'mandatory' => 0 },
#          'sU' => {
#            'value'     => 'root',
#            'desc'      => 'SSH User',
#            'default'   => '',
#            'mandatory' => 0
#          },
#          'sK' => { 'desc' => 'SSH Key', 'default' => '', 'mandatory' => 0 },
#          'P' => { 'desc' => 'Port', 'default' => '3306', 'mandatory' => 0 },
#          'u' => {
#            'value'     => 'root',
#            'desc'      => 'Username',
#            'default'   => '',
#            'mandatory' => 0
#          },
#          'p' => { 'desc' => 'Password', 'default' => '', 'mandatory' => 0 },
#          'h' => {
#            'value'     => '10.139.44.206',
#            'desc'      => 'Hostname',
#            'default'   => '',
#            'mandatory' => 0
#          },
#          'D' =>
#            { 'desc' => 'Database name', 'default' => '', 'mandatory' => 0 },
#          'G' => {
#            'desc'      => 'Defaults File Group',
#            'default'   => 'client',
#            'mandatory' => 0
#          },
#          't' => { 'desc' => 'Table name', 'default' => '', 'mandatory' => 0 }
#        },
#        'DSN'
#      ),
#      'crm' => '/usr/sbin/crm',
#      'cib' => {
#        'epoch'           => '49',
#        'admin_epoch'     => '0',
#        'crm_feature_set' => '3.0.1',
#        'num_updates'     => '8',
#        'have-quorum'     => '1',
#        'dc-uuid'         => '073af61d-da9f-49ea-afe0-2199c952bb3e',
#        'validate-with'   => 'pacemaker-1.0',
#        'configuration'   => {
#          'op_defaults' => {},
#          'resources'   => {
#            'group' => {
#              'VIP1' => {
#                'primitive' => {
#                  'ip1arp' => {
#                    'provider'            => 'heartbeat',
#                    'instance_attributes' => {
#                      'nvpair' => {
#                        'ip' => {
#                          'value' => '192.168.5.30',
#                          'id'    => 'ip1arp-instance_attributes-ip'
#                        },
#                        'nic' => {
#                          'value' => 'eth0:5',
#                          'id'    => 'ip1arp-instance_attributes-nic'
#                        }
#                      },
#                      'id' => 'ip1arp-instance_attributes'
#                    },
#                    'class' => 'ocf',
#                    'type'  => 'SendArp'
#                  },
#                  'ip1' => {
#                    'provider'            => 'heartbeat',
#                    'instance_attributes' => {
#                      'nvpair' => {
#                        'ip' => {
#                          'value' => '192.168.5.30',
#                          'id'    => 'ip1-instance_attributes-ip'
#                        },
#                        'nic' => {
#                          'value' => 'eth0:5',
#                          'id'    => 'ip1-instance_attributes-nic'
#                        }
#                      },
#                      'id' => 'ip1-instance_attributes'
#                    },
#                    'class' => 'ocf',
#                    'type'  => 'IPaddr2'
#                  }
#                }
#              },
#              'VIP2' => {
#                'primitive' => {
#                  'provider' => 'heartbeat',
#                  'id'       => 'dum1',
#                  'type'     => 'Dummy',
#                  'class'    => 'ocf'
#                }
#              }
#            }
#          },
#          'constraints' => {
#            'rsc_location' => {
#              'rsc'  => 'VIP1',
#              'id'   => 'cli-prefer-VIP1',
#              'rule' => {
#                'expression' => {
#                  'attribute' => '#uname',
#                  'value'     => 'centos-vm-1',
#                  'id'        => 'cli-prefer-expr-VIP1',
#                  'operation' => 'eq'
#                },
#                'id'    => 'cli-prefer-rule-VIP1',
#                'score' => 'INFINITY'
#              }
#            }
#          },
#          'rsc_defaults' => {
#            'meta_attributes' => {
#              'nvpair' => {
#                'value' => '100',
#                'name'  => 'resource-stickiness',
#                'id'    => 'rsc-options-resource-stickiness'
#              },
#              'id' => 'rsc-options'
#            }
#          },
#          'nodes' => {
#            'node' => {
#              'a5ea718d-d265-4cda-8471-cfd42f3a818b' => {
#                'uname'               => 'centos-vm-1',
#                'instance_attributes' => {
#                  'nvpair' => {
#                    'value' => 'off',
#                    'name'  => 'standby',
#                    'id' => 'nodes-a5ea718d-d265-4cda-8471-cfd42f3a818b-standby'
#                  },
#                  'id' => 'nodes-a5ea718d-d265-4cda-8471-cfd42f3a818b'
#                },
#                'type' => 'normal'
#              },
#              '073af61d-da9f-49ea-afe0-2199c952bb3e' =>
#                { 'uname' => 'centos-vm-2', 'type' => 'normal' }
#            }
#          },
#          'crm_config' => {
#            'cluster_property_set' => {
#              'nvpair' => {
#                'stonith-enabled' => {
#                  'value' => 'false',
#                  'id'    => 'cib-bootstrap-options-stonith-enabled'
#                },
#                'cluster-infrastructure' => {
#                  'value' => 'Heartbeat',
#                  'id'    => 'cib-bootstrap-options-cluster-infrastructure'
#                },
#                'expected-quorum-votes' => {
#                  'value' => '1',
#                  'id'    => 'cib-bootstrap-options-expected-quorum-votes'
#                },
#                'no-quorum-policy' => {
#                  'value' => 'ignore',
#                  'id'    => 'cib-bootstrap-options-no-quorum-policy'
#                },
#                'dc-version' => {
#                  'value' => '1.0.9-89bd754939df5150de7cd76835f98fe90851b677',
#                  'id'    => 'cib-bootstrap-options-dc-version'
#                }
#              },
#              'id' => 'cib-bootstrap-options'
#            }
#          }
#        }
#      },
#      'vip-timeout' => '300',
#      'vip'         => 'VIP1',
#      'primary_dsn' => bless(
#        {
#          'F' =>
#            { 'desc' => 'Defaults File', 'default' => '', 'mandatory' => 0 },
#          'S' => { 'desc' => 'Socket path', 'default' => '', 'mandatory' => 0 },
#          'sU' => {
#            'value'     => 'root',
#            'desc'      => 'SSH User',
#            'default'   => '',
#            'mandatory' => 0
#          },
#          'sK' => { 'desc' => 'SSH Key', 'default' => '', 'mandatory' => 0 },
#          'P' => { 'desc' => 'Port', 'default' => '3306', 'mandatory' => 0 },
#          'u' => {
#            'value'     => 'root',
#            'desc'      => 'Username',
#            'default'   => '',
#            'mandatory' => 0
#          },
#          'p' => { 'desc' => 'Password', 'default' => '', 'mandatory' => 0 },
#          'h' => {
#            'value'     => '10.139.44.223',
#            'desc'      => 'Hostname',
#            'default'   => '',
#            'mandatory' => 0
#          },
#          'D' =>
#            { 'desc' => 'Database name', 'default' => '', 'mandatory' => 0 },
#          'G' => {
#            'desc'      => 'Defaults File Group',
#            'default'   => 'client',
#            'mandatory' => 0
#          },
#          't' => { 'desc' => 'Table name', 'default' => '', 'mandatory' => 0 }
#        },
#        'DSN'
#      ),
#      'crm_mon' => '/usr/sbin/crm_mon'
#    },
#    'LHAMoveVIP'
#  )
#  ]

1;
