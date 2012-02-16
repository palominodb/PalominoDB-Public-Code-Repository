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