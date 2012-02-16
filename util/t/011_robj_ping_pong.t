use strict;
use warnings FATAL => 'all';
use Test::More tests => 5;
use TestUtil;

BEGIN {
  use_ok('RObj');
}

SKIP: {
  if(!$ENV{TEST_ROBJ_HOST} or !$ENV{TEST_ROBJ_USER}) {
    skip "Need TEST_ROBJ_HOST and TEST_ROBJ_USER setup", 4
  }
  my $ro = new_ok('RObj' => [$ENV{TEST_ROBJ_HOST}, $ENV{TEST_ROBJ_USER}]);
  $ro->add_main(
    sub {
      my $cmd = $_[0];
      while($cmd ne 'DONE') {
        ($cmd) = R_read();
        if($cmd eq 'PING') {
          R_print('PONG');
        }
        elsif($cmd eq 'DONE') {
          # NOOP
        }
        else {
          R_print('PANG');
        }
      }
      return OK;
    }
  );

  $ro->start('START');
  $ro->write('PING');
  my ($resp) = $ro->read();
  is('PONG', $resp, 'get pong');
  $ro->write('PINK');
  ($resp) = $ro->read();
  is('PANG', $resp, 'get pang');
  $ro->write('DONE');
  my @r =$ro->wait();
  is_deeply(\@r, ['EXIT', OK], 'get exit');
}
