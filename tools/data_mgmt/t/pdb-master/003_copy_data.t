# 003_copy_data.t
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
use Test::More tests => 10;
use File::Path qw(rmtree);
use Cwd 'abs_path';

BEGIN {
  require_ok('src/pdb-master');
  mkdir('t_srcdir');
  mkdir('t_srcdir/data');
  for(qw(one two three)) {
    open my $fh, ">t_srcdir/data/$_" or die('Unable to create test data "'. $_ .'"');
    print $fh "$_\n";
    close($fh);
  }
  mkdir('t_dstdir');
}

END {
  rmtree('t_srcdir');
  rmtree('t_dstdir');
}

my $ssh_user = $ENV{'TEST_SSH_USER'} || $ENV{'LOGNAME'};
my $ssh_host = 'localhost';
my $ssh_key  = $ENV{'TEST_SSH_KEY'};

for(qw(one two three)) {
  ok(-f "t_srcdir/data/$_", "srcdata $_ exists");
}

my $r = pdb_master::copy_data(1, $ssh_host, $ssh_user, $ssh_key, abs_path('t_srcdir'), abs_path('t_dstdir'));

for(qw(one two three)) {
  ok(! -f "t_dstdir/data/$_", "dst data $_ does not exist with dryrun");
}

$r = pdb_master::copy_data(0, $ssh_host, $ssh_user, $ssh_key, abs_path('t_srcdir'), abs_path('t_dstdir'));

is($r, 0, 'copy_data returned ok');

for(qw(one two three)) {
  ok(-f "t_dstdir/data/$_", "dst data $_ does exist without dryrun");
}
