use strict;
use warnings FATAL => 'all';
use Test::More tests => 5;
use TestUtil;
use MysqlInstance;

SKIP: {
  skip 'this only valid on FreeBSD', 5 if($^O ne 'freebsd');
  my $mths = MysqlInstance::Methods->detect;
  is($mths->{start}, '/etc/init.d/mysql start &>/dev/null', 'start cmd');
  is($mths->{stop}, '/etc/init.d/mysql stop &>/dev/null', 'stop cmd');
  is($mths->{restart}, '/etc/init.d/mysql restart &>/dev/null', 'restart cmd');
  is($mths->{status}, '/etc/init.d/mysql status &>/dev/null', 'restart cmd');
  is($mths->{config}, '/etc/mysql/my.cnf', 'config path');
  my $server_pkg;
  chomp($server_pkg = qx{pkg_info | grep mysql-server});
  diag("It's OK to ignore these failures since you don't appear to have mysql-server installed") unless($server_pkg);
}
