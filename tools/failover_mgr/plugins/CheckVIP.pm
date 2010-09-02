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
package LinuxHA_VIP;
use strict;
use warnings FATAL => 'all';
use Exporter;
use Carp;
our @ISA = qw(FailoverPlugin);

# Default path to control files
my $LinuxHA_VIP_CTL_Path = "/var/lib/vip";

sub _ctl_path_real {
  my ($path) = @_;
  if(-d $path) {
    return 1;
  }
  else {
    return 0;
  }
}

## Routine to touch a LinuxHA control file
## This and the next sub are separated out for testing.
sub _touch_ctl {
  my ($path) = @_;
  return( system("touch $path 2>/dev/null") >> 8 );
}

## Routine to remove a LinuxHA control file
sub _remove_ctl {
  my ($path) = @_;
  return( unlink($path) );
}

## Waits for $vip to show up in the output of ifconfig
sub _wait_vip {
  my ($vip, $timeout) = @_;
  my $t=0;
  while(system("ifconfig | grep -q $vip") >> 8) {
    sleep 1;
    if($t++ > $timeout) {
      return 1;
    }
  }
  return 0;
}

sub new {
  my ($class, $opts) = @_;
  $$opts{'vip-timeout'} ||= 300; # Default to 5 minute VIP move timeout
  if(not exists $$opts{'vip'}) {
    die("--vip option required for LinuxHA_VIP plugin");
  }
  return bless $class->SUPER::new($opts), $class;
}

sub options {
  return ('vip=s', 'vip-timeout=i');
}

sub pre_verification {
  my ($self, $pridsn, $faildsn) = @_;
  my $check_vip = RObj->new($pridsn->get('h'), $pridsn->get('sU') || $ENV{USER}, $pridsn->get('sK'));
  $check_vip->add_main(\&_wait_vip);
  my $test_dir = $check_vip->copy();
  $test_dir->add_main(\&_ctl_path_real);
  my $r = [$test_dir->do($LinuxHA_VIP_CTL_Path)];
  if(!$r->[1]) {
    die("$LinuxHA_VIP_CTL_Path does not exist on ". $pridsn->get('h'));
  } 
  
  $r = [$check_vip->do($$self{'vip'}, 0)];
  if($r->[1] > 0) {
    die("$$self{'vip'} not on primary before failover");
  }
}

sub finish_failover {
    my ($self, $res, $pridsn, $faildsn) = @_;
    my $r = undef;
    
    if($res < 1) {
      $::PLOG->e("Not doing VIP failover due to main failover failure.");
      return 1;
    }
    
    my $remove_pri = RObj->new($pridsn->get('h'), $pridsn->get('sU') || $ENV{USER}, $pridsn->get('sK'));
    my $touch_fail = RObj->new($faildsn->get('h'), $faildsn->get('sU') || $ENV{USER}, $faildsn->get('sK'));
    my $wait_fail  = $touch_fail->copy();
    $remove_pri->add_main(\&_remove_ctl);
    $touch_fail->add_main(\&_touch_ctl);
    $wait_fail->add_main(\&_wait_vip);
    
    $r = [$remove_pri->do($LinuxHA_VIP_CTL_Path . '/' . $$self{'vip'})];
    if($r->[1] < 1) {
      die("failed to remove primary control file");
    }
    $r = [$touch_fail->do($LinuxHA_VIP_CTL_Path . '/' . $$self{'vip'})];
    if($r->[1] > 0) {
      die("failed to set failover control file");
    }
    
    $r = [$wait_fail->do($$self{'vip'}, $$self{'vip-timeout'})];
    if($r->[1] > 0) {
      die("$$self{'vip'} did not come up after $$self{'vip-timeout'} seconds");
    }

}

sub post_verification {
  my ($self, $res, $pridsn, $faildsn) = @_;
  my $dbh = undef;
  my $vipdsn = DSN->_create($pridsn);
  $vipdsn->{'h'}->{'value'} = $$self{'vip'};
  eval {
    $dbh = $vipdsn->get_dbh();
  };
  if($@) {
    $::PLOG->e("Unable to access MySQL through the VIP!");
  }
}

1;
