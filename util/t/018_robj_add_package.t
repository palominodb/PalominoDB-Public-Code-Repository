use strict;
use warnings FATAL => 'all';
use RObj;
use MysqlMasterInfo;
use Test::More tests => 1;

my $ssh_host = $ENV{TEST_SSH_HOST};
my $ssh_user = $ENV{TEST_SSH_USER};


SKIP: {
  skip 'Need test SSH setup.', 1 if(!$ssh_host or !$ssh_user);
  my $rmi = RObj->new($ssh_host, $ssh_user);
  $rmi->add_package('MysqlMasterInfo');
  $rmi->add_main(sub { MysqlMasterInfo->open('/fake/master.info'); });
  is_deeply([$rmi->do()], ['EXIT', MysqlMasterInfo->open('/fake/master.info')], 'remote master info');
}
