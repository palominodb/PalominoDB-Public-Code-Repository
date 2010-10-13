use strict;
use warnings FATAL => 'all';
use Test::More tests => 8;
use TestUtil;

BEGIN {
  use_ok('RObj');
}

sub nondefault_fail_check {
  if( -d "/noway" ) {
    R_print("ok");
  }
  else {
    R_print("/noway is not a directory!\n");
  }
  return OK;
}

sub nondefault_pass_check {
  if( -d "/etc" ) {
    R_print("ok");
  }
  else {
    R_print("/etc not a directory!");
  }
  return OK;
}

SKIP: {
  if(!$ENV{TEST_SSH_HOST} or !$ENV{TEST_SSH_USER}) {
    skip "Need TEST_SSH_HOST and TEST_SSH_USER setup", 5
  }
  my $ro = new_ok('RObj' => [$ENV{TEST_SSH_HOST}, $ENV{TEST_SSH_USER}]);
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

  eval {
    $ro->check();
  };
  diag($@ or "no exception");
  unlike($@, qr/^failed check:/, 'default check() returns successfully');

  eval {
    $ro->check(\&nondefault_pass_check);
  };
  diag($@ or "no exception");
  unlike($@, qr/^failed check:/, 'non-default check() returns successfully');

  eval {
    $ro->check(\&nondefault_fail_check);
  };
  diag($@ or "no exception");
  like($@, qr/^failed check:/, 'non-default check() fails correctly');
}
