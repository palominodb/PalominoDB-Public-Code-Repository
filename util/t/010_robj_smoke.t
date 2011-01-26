use strict;
use warnings FATAL => 'all';
use Test::More tests => 5;
use TestUtil;
use DSN;

BEGIN {
  use_ok('RObj');
}

SKIP: {
  skip "Need TEST_SSH_HOST and TEST_SSH_USER setup", 2 if !$ENV{TEST_SSH_HOST} or !$ENV{TEST_SSH_USER};
  my $dsnp = DSNParser->default();
  my $dsn  = $dsnp->parse("h=$ENV{TEST_SSH_HOST},sU=$ENV{TEST_SSH_USER}"); 
  my $ro = new_ok('RObj' => [$ENV{TEST_SSH_HOST}, $ENV{TEST_SSH_USER}]);
  $ro->add_main(sub { return 0; });
  my @r = $ro->do(0);
  is_deeply(\@r, ['EXIT', 0], 'RObj exits with 0');
  
  $ro = new_ok('RObj', => [$dsn]);
  $ro->add_main(sub { return 1; });
  @r = $ro->do(0);
  is_deeply(\@r, ['EXIT', 1], 'RObj from DSN exits with 1');
}
