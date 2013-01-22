# 008_robj_base.t
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
use Data::Dumper;
use TestUtil;
BEGIN {
  use_ok('RObj::Base');
}
use Fcntl qw(:seek);

my ($rb, $fh, $content, @r);
$rb = new_ok( 'RObj::Base' );

open($fh, "+>", undef);
$rb->write_message( $fh, ["test1", "test2"] );
seek($fh, 0, SEEK_SET);
sysread($fh, $content, 10240);
is($content, 'BQcCAAAAAQQCAAAAAgoFdGVzdDEKBXRlc3Qy
ok
', 'test array serialized');

seek($fh, 0, SEEK_SET);
@r = $rb->read_message( $fh );
is_deeply(\@r, [ ["test1", "test2"] ], "test array deserialized");


$rb->write_message( $fh, { test3 => 'test4' } );
seek($fh, 0, SEEK_SET);
@r = $rb->read_message( $fh );
is_deeply(\@r, [ ['test1', 'test2'], { test3 => 'test4' } ], "can parse many messages");
close($fh);
