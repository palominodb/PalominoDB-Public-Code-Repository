use strict;
use warnings FATAL => 'all';
use Test::More tests => 3;
use TestUtil;

BEGIN {
  use_ok('RObj');
}

SKIP: {
  skip "Need TEST_ROBJ_HOST and TEST_ROBJ_USER setup", 2 if !$ENV{TEST_ROBJ_HOST} or !$ENV{TEST_ROBJ_USER};
  my $ro = new_ok('RObj' => [$ENV{TEST_ROBJ_HOST}, $ENV{TEST_ROBJ_USER}]);
  $ro->add_main(sub { return 0; });
  my @r = $ro->do(0);
  is_deeply(\@r, ['EXIT', 0], 'RObj exits with 0');
}
