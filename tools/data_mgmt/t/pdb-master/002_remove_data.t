# 002_remove_data.t
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
use Test::More tests => 11;
use File::Path qw(rmtree);

BEGIN {
  require_ok('src/pdb-master');
  mkdir("testdir");
  mkdir("testdir/empty_dir");
  system("echo > testdir/file1");
  system("echo > testdir/file2");
}

END {
  rmtree("testdir");
}

eval {
  pdb_master::remove_datadir(1, 'testdir');
};
is($@, '', 'remove_datadir did not die');
ok(-d "testdir", "testdir exists");
ok(-d "testdir/empty_dir", "empty_dir exists");
ok(-f "testdir/file1", "file1 exists beneath testdir");
ok(-f "testdir/file2", "file2 exists beneath testdir");
eval {
  pdb_master::remove_datadir(0, 'testdir');
};
is($@, '', 'remove_datadir did not die');
ok(-d "testdir", "testdir exists");
ok(! -d "testdir/empty_dir", "empty_dir was deleted");
ok(! -f "testdir/file1", "file1 was deleted");
ok(! -f "testdir/file2", "file2 was deleted");
