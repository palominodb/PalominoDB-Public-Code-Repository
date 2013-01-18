# 000_parse_hostref.t
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
use Test::More tests => 7;

BEGIN {
  require_ok('src/pdb-master');
}

is_deeply(scalar pdb_master::parse_hostref('user1@host1:/path1'),
  ['user1', 'host1', '/path1'], 'full set');
is_deeply(scalar pdb_master::parse_hostref('host1:/path1'),
  [undef, 'host1', '/path1'], 'host and path');
is_deeply(scalar pdb_master::parse_hostref('user1@host1'),
  ['user1', 'host1', undef], 'user and host');
is_deeply(scalar pdb_master::parse_hostref('host1'),
  [undef, 'host1', undef], 'just host');


isnt(pdb_master::parse_hostref(''), '', 'empty ref');
ok(not pdb_master::parse_hostref('user@:/path1'), 'missing host');
