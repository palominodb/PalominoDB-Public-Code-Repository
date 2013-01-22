# 021_dsnparser.t
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
use English qw(-no_match_vars);
use Test::More tests => 5;

BEGIN {
  use_ok('DSN');
}

my $p = DSNParser->new({});
eval {
  $p->parse('a=v');
};
like($EVAL_ERROR, qr/^Unknown key/, 'minimal/empty parser rejects everything');

$p = DSNParser->new({ 'h' => { 'mandatory' => 1, 'default' => '' } });

eval { $p->parse('h=localhost'); };
ok(!$EVAL_ERROR, 'simple/single-key parser accepts that key');

eval { $p->parse('h=localhost,u=naw'); };
like($EVAL_ERROR, qr/^Unknown key/, 'simple/single-key parser rejects other keys');

$p = DSNParser->new({
    'h' => { 'mandatory' => 1, 'default' => '' },
    'u' => { 'mandatory' => 0, 'default' => '' }
  });
eval { $p->parse('u=naw'); };
like($EVAL_ERROR, qr/^Missing key/, 'simple/double-key parser dies on missing mandatory keys');
