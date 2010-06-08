use strict;
use warnings FATAL => 'all';
use Test::More tests => 6;

BEGIN {
  use_ok('Path');
  mkdir("testdir");
  mkdir("testdir/empty_dir");
  system("echo > testdir/file1");
  system("echo > testdir/file2");
}

END {
  rmdir("testdir");
}

eval {
  Path::dir_empty('testdir');
};
is($@, '', 'remove_datadir did not die');
ok(-d "testdir", "testdir exists");
ok(! -d "testdir/empty_dir", "empty_dir was deleted");
ok(! -f "testdir/file1", "file1 was deleted");
ok(! -f "testdir/file2", "file2 was deleted");
