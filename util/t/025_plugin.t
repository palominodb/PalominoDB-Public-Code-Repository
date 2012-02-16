use strict;
use warnings FATAL => 'all';
use Test::More tests => 9;

BEGIN {
  use TestUtil;
  use lib get_files_dir() . "/plugin";
  use_ok('Plugin');
}

ok(Plugin::load('TestMod'), 'TestMod loaded');
eval {
  ok(TestMod::ok(), 'TestMod::ok() returns true');
};
if($@) { fail('TestMod::ok() returns true'); }

ok(Plugin::load('TestMod2', 'frog'), 'TestMod2 loaded using frog()');
eval {
  ok(TestMod2::ok(), 'TestMod2::ok() returns true');
};
if($@) { fail('TestMod2::ok() returns true'); }

ok(!Plugin::load('TestMod3'), 'Fails to load using new()');

ok(Plugin::load('TestMod'), 'TestMod still loaded');
ok(!Plugin::load('NotRealMod'), 'failure to load NotRealMod');
like($@, qr/Can't locate/, '$@ still set after failure');
diag($@);
