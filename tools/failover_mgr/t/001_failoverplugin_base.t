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
