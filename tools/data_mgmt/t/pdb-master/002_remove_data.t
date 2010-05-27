use strict;
use warnings FATAL => 'all';
use Test::More tests => 11;
use File::Path qw(rmtree);

BEGIN {
  require_ok('src/pdb-master.in.pl');
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
