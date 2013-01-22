# Lockfile.t
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
use Test::More tests => 8;
use Time::HiRes qw(gettimeofday tv_interval);
use Data::Dumper;
use Fcntl qw(:DEFAULT :flock);
$Data::Dumper::Indent = 0;

BEGIN {
	use_ok('Lockfile');
}

{
	my $t = Lockfile->get('test1.lock');
	ok(-f "test1.lock", "lockfile created");
}
ok(! -f "test1.lock", "lockfile removed outside of scope");

{
	my $t0 = [gettimeofday()];
	my $d  = 0.0;
	my $t = Lockfile->get('test2.lock');
	eval {
		my $t1 = Lockfile->get('test2.lock', 1);
	};
	like("$@", qr/^Lockfile: Unable to acquire lock on.*after 1 seconds/, "lock timeout works");
	$d = tv_interval($t0);
	cmp_ok($d, '>=', 1, 'timeout was of expected length');
}
ok(! -f "test2.lock", "lockfile removed in spite of 'deadlock'");

{
	eval {
		my $t = Lockfile->get('/not-a-real-directory/test3.lock');
	};
	like("$@", qr/^Lockfile: Unable to open lock/, "exception on invalid path");
}

{
	eval {
		my $t = Lockfile->get('test4.lock');
		my $fh = $$t{'fh'};
		close($fh);
	};
	my $e = "$@";
	TODO: {
		local $TODO = "Should produce an exception but currently doesn't?";
		like($e, qr/^Lockfile:/, "unable to cleanup locks");
	}
}
