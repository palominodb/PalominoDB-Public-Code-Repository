# 004_linuxha_vip_plugin.t
# Copyright (C) 2013 PalominoDB, Inc.
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

use strict;
use warnings FATAL => 'all';
use TestUtil;
use Test::More tests => 9;
use RObj;
use ProcessLog;
use FailoverPlugin;
use LinuxHA_VIP;

BEGIN {
  my $pl = ProcessLog->new($0, 'pdb-test-harness');
  no strict 'refs';
  *::PLOG = \$pl;
}

my $VIP = "127.0.0.8";

my $Linux_ifconfig_add = "sudo ifconfig lo add $VIP";
my $Linux_ifconfig_del = "sudo ifconfig lo:0 down";

my $Mac_ifconfig_add = "sudo ifconfig lo0 alias $VIP";
my $Mac_ifconfig_del = "sudo ifconfig lo0 -alias $VIP";

my $ifconfig_add = $^O eq 'darwin' ? $Mac_ifconfig_add : $Linux_ifconfig_add;
my $ifconfig_del = $^O eq 'darwin' ? $Mac_ifconfig_del : $Linux_ifconfig_del;

{
  no warnings 'once';
  $LinuxHA_VIP::LinuxHA_VIP_CTL_Path = './';
}

my $vip_plug = new_ok('LinuxHA_VIP', [{'vip-timeout' => 3, 'vip' => $VIP}]);

ok(LinuxHA_VIP::_ctl_path_real('.'), 'ctl path real');
ok(!LinuxHA_VIP::_ctl_path_real('./notreal'), 'ctl path notreal');

ok(LinuxHA_VIP::_touch_ctl("./$VIP") == 0, 'touch ctl');
ok(LinuxHA_VIP::_touch_ctl("./notreal/$VIP") == 1, 'notreal touch ctl');

ok(LinuxHA_VIP::_remove_ctl("./$VIP"), 'remove ctl');
ok(!LinuxHA_VIP::_remove_ctl("./notreal/$VIP"), 'notreal remove ctl');

ok(LinuxHA_VIP::_wait_vip($VIP, 0) == 1, 'wait vip no-vip');
system($ifconfig_add);
ok(LinuxHA_VIP::_wait_vip($VIP, 0) == 0, 'wait vip with-vip');
system($ifconfig_del);
