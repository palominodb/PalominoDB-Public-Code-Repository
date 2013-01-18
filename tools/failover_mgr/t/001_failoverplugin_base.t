# 001_failoverplugin_base.t
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
use Test::More tests => 6;
use ProcessLog;
use TestUtil;
use TestDB;
use FailoverPlugin;

BEGIN {
  my $pl = ProcessLog->new($0, 'pdb-test-harness');
  no strict 'refs';
  *::PLOG = \$pl;
}

my $fp = new_ok('FailoverPlugin');
is($fp->pre_verification, undef, 'pre-verfication base does nothing');
is($fp->post_verification, undef, 'post-verfication base does nothing');
is($fp->begin_failover, undef, 'begin-failover base does nothing');
is($fp->finish_failover, undef, 'finish-failover base does nothing');
is_deeply([$fp->options], [], 'options is empty');
