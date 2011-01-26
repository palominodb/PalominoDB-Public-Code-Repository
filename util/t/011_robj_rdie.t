BEGIN {
  die('Need TEST_SSH_HOST and TEST_SSH_USER')
    unless($ENV{TEST_SSH_HOST} and $ENV{TEST_SSH_USER});
}
use strict;
use warnings FATAL => 'all';
use Test::More tests => 7;
use TestUtil;

BEGIN {
  use_ok('RObj');
}

my $ro = new_ok('RObj' => [$ENV{TEST_SSH_HOST}, $ENV{TEST_SSH_USER}]);
$ro->add_main(sub {
    die('Test death');
  }
);
my @res = $ro->do();
like($res[0], qr/Test death/, 'get death from native');
is($res[-1], -3, 'get error -3');

$ro = new_ok('RObj' => [$ENV{TEST_SSH_HOST}, $ENV{TEST_SSH_USER}]);
$ro->add_main(sub {
    eval {
      die('eval death');
    };
    return 'ok';
  }
);

@res = $ro->do();
like($res[0], qr/EXIT/, 'no death from inside eval');
like($res[1], qr/ok/, 'no death from inside eval');
