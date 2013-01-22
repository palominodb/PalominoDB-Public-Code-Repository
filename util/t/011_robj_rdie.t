# 011_robj_rdie.t
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

BEGIN {
  die('Need TEST_SSH_HOST and TEST_SSH_USER')
    unless($ENV{TEST_SSH_HOST} and $ENV{TEST_SSH_USER});
}
use strict;
use warnings FATAL => 'all';
use Test::More tests => 7;
use TestUtil;

BEGIN {
  use_ok('RObj');
}

my $ro = new_ok('RObj' => [$ENV{TEST_SSH_HOST}, $ENV{TEST_SSH_USER}]);
$ro->add_main(sub {
    die('Test death');
  }
);
my @res = $ro->do();
like($res[0], qr/Test death/, 'get death from native');
is($res[-1], -3, 'get error -3');

$ro = new_ok('RObj' => [$ENV{TEST_SSH_HOST}, $ENV{TEST_SSH_USER}]);
$ro->add_main(sub {
    eval {
      die('eval death');
    };
    return 'ok';
  }
);

@res = $ro->do();
like($res[0], qr/EXIT/, 'no death from inside eval');
like($res[1], qr/ok/, 'no death from inside eval');
