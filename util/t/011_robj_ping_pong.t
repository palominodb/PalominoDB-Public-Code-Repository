# 011_robj_ping_pong.t
# Copyright (C) 2013 PalominoDB, Inc.
# 
# You may contact the maintainers at eng@palominodb.com.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

use strict;
use warnings FATAL => 'all';
use Test::More tests => 8;
use TestUtil;

BEGIN {
  use_ok('RObj');
}

sub nondefault_fail_check {
  if( -d "/noway" ) {
    return 'ok';
  }
  return "/noway is not a directory!\n";
}

sub nondefault_pass_check {
  if( -d "/etc" ) {
    return 'ok';
  }
  return "/etc is not a directory?";
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
    diag('check result:', join(' ', $ro->check()));
  };
  diag($@ or "no exception");
  unlike($@, qr/^failed check:/, 'default check() returns successfully');

  eval {
     diag('check result:', join(' ', $ro->check(\&nondefault_pass_check)));
  };
  diag($@ or "no exception");
  unlike($@, qr/^failed check:/, 'non-default check() returns successfully');

  eval {
     diag('check result:', join(' ', $ro->check(\&nondefault_fail_check)));
  };
  diag($@ or "no exception");
  like($@, qr/^failed check:/, 'non-default check() fails correctly');
}
