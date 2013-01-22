# 010_robj_smoke.t
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
use Test::More tests => 5;
use TestUtil;
use DSN;

BEGIN {
  use_ok('RObj');
}

SKIP: {
  skip "Need TEST_SSH_HOST and TEST_SSH_USER setup", 2 if !$ENV{TEST_SSH_HOST} or !$ENV{TEST_SSH_USER};
  my $dsnp = DSNParser->default();
  my $dsn  = $dsnp->parse("h=$ENV{TEST_SSH_HOST},sU=$ENV{TEST_SSH_USER}"); 
  my $ro = new_ok('RObj' => [$ENV{TEST_SSH_HOST}, $ENV{TEST_SSH_USER}]);
  $ro->add_main(sub { return 0; });
  my @r = $ro->do(0);
  is_deeply(\@r, ['EXIT', 0], 'RObj exits with 0');
  
  $ro = new_ok('RObj', => [$dsn]);
  $ro->add_main(sub { return 1; });
  @r = $ro->do(0);
  is_deeply(\@r, ['EXIT', 1], 'RObj from DSN exits with 1');
}
