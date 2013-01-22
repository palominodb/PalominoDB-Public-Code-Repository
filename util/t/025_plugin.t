# 025_plugin.t
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
use Test::More tests => 9;

BEGIN {
  use TestUtil;
  use lib get_files_dir() . "/plugin";
  use_ok('Plugin');
}

ok(Plugin::load('TestMod'), 'TestMod loaded');
eval {
  ok(TestMod::ok(), 'TestMod::ok() returns true');
};
if($@) { fail('TestMod::ok() returns true'); }

ok(Plugin::load('TestMod2', 'frog'), 'TestMod2 loaded using frog()');
eval {
  ok(TestMod2::ok(), 'TestMod2::ok() returns true');
};
if($@) { fail('TestMod2::ok() returns true'); }

ok(!Plugin::load('TestMod3'), 'Fails to load using new()');

ok(Plugin::load('TestMod'), 'TestMod still loaded');
ok(!Plugin::load('NotRealMod'), 'failure to load NotRealMod');
like($@, qr/Can't locate/, '$@ still set after failure');
diag($@);
