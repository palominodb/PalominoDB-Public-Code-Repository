use strict;
use warnings FATAL => 'all';
use Test::More tests => 6;
use TestUtil;
BEGIN {
  use_ok('MysqlInstance');
}

my $ssh_host = $ENV{TEST_SSH_HOST};
my $ssh_user = $ENV{TEST_SSH_USER};

my $l = new_ok('MysqlInstance' => ['localhost']);

my $res = MysqlInstance::_action(undef, 'hostname');
is($res->[0], 'debian-lenny.i.linuxfood.net', '_action has right hostname');

is_deeply($l->_do('hostname'), 'debian-lenny.i.linuxfood.net', '_do has right hostname');

SKIP: {
  skip 'Need TEST_SSH_HOST and TEST_SSH_USER setup', 2 if(!$ssh_host or !$ssh_user);
  my $r = new_ok('MysqlInstance' => [$ssh_host, $ssh_user]);
  is($r->_do('hostname'), $ssh_host, 'remote hostname matches');
}
