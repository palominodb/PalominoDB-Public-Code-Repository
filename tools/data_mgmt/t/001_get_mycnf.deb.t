use strict;
use warnings FATAL => 'all';
use Test::More tests => 2;

BEGIN {
  require_ok('src/pdb-master.in.pl');
  use_ok('IniFile');
}

SKIP: {
  skip "not on debian", 2 if(! -f "/etc/debian_version");
  ok(-f "/etc/mysql/my.cnf", 'my.cnf present');
  is_deeply(pdb_master::get_mycnf(), {}, "loaded my.cnf correctly");
}
