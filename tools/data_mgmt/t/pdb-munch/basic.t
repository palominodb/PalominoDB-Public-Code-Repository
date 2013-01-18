# basic.t
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
use TestDB;
use TestUtil;
use Test::More tests => 6;

my @COMMON_ARGS = ('--logfile=pdb-test-harness');

require_ok('src/pdb-munch');
is(pdb_munch::main(@COMMON_ARGS, '--dump-spec'), 0, '--dump-spec returns 0');
ok(-f "default_spec.conf", "default_spec.conf exists");
is(pdb_munch::main(@COMMON_ARGS), 1, 'missing --spec returns 1');
is(pdb_munch::main(@COMMON_ARGS, '--spec=default_spec.conf'), 1, 'missing --config returns 1');
open my $fh, ">test.conf";
print $fh <<EOF;
[__connection__]
dsn = $TestDB::dsnstr,D=munch

[foo]
c1 = name
c2 = value
EOF
close($fh);

push @COMMON_ARGS, ('--spec=default_spec.conf', '--config=test.conf');

is(pdb_munch::main(@COMMON_ARGS), 1, 'missing spec source handled');

END {
  unlink('default_spec.conf');
  unlink('test.conf');
  unlink('t/pdb-munch/basic.t.log');
}
