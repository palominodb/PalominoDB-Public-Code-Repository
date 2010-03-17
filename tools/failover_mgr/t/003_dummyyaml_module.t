use strict;
use warnings FATAL => 'all';
no warnings 'once';
use Test::More tests => 5;
use TestUtil;

BEGIN {
  require_ok($ENV{TOOL});
  fake_use('DSN.pm');
}

my @opts = (
  '-L', 'pdb-test-harness',
  '-m', 'DummyYAML',
  '--dsn', get_files_dir() .'/dsn.yml',
  '--cluster', 'test'
);
eval {
  FailoverManager::main(@opts);
};
is($@, '', 'no croak');
is_deeply(\%DummyYAML::failed_over, {'h=primary_s,u=msandbox,p=msandbox' => 1,'h=failover_s,u=msandbox,p=msandbox' => 1, 'h=slave_s1,u=msandbox,p=msandbox' => 1, 'h=slave_s2,u=msandbox,p=msandbox' => 1}, 'marks slaves as failed over');

%DummyYAML::failed_over = ();
eval {
  unshift @opts, '--pretend';
  FailoverManager::main(@opts);
};
is($@, '', 'no croak');
is_deeply(\%DummyYAML::failed_over, {}, 'marks nothing as failed over');
