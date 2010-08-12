use strict;
use warnings FATAL => 'all';
use Test::More tests => 8;
use TestUtil;
BEGIN {
  use_ok('YAMLDSN');
}

my $ydsn = new_ok('YAMLDSN');

eval {
  $ydsn->open(TestUtil::get_files_dir() .'/yamldsn/local_dsn.yml');
};
ok(!$@, 'parse basic');

eval {
  $ydsn->open(TestUtil::get_files_dir() .'/yamldsn/custom_keys_dsn.yml');
};
ok(!$@, 'parse with custom keys');

eval {
  is($ydsn->server_checksum('s1'), 1, 'checksum key is y');
};

ok(!$@, 'server custom key');

eval {
  is($ydsn->cluster_backup('c1'), 1, 'backup key is y');
};

ok(!$@, 'cluster custom key');
