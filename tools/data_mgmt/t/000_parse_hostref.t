use strict;
use warnings FATAL => 'all';
use Test::More tests => 7;

BEGIN {
  require_ok('src/pdb-master.in.pl');
}

is_deeply(scalar pdb_master::parse_hostref('user1@host1:/path1'),
  ['user1', 'host1', '/path1'], 'full set');
is_deeply(scalar pdb_master::parse_hostref('host1:/path1'),
  [undef, 'host1', '/path1'], 'host and path');
is_deeply(scalar pdb_master::parse_hostref('user1@host1'),
  ['user1', 'host1', undef], 'user and host');
is_deeply(scalar pdb_master::parse_hostref('host1'),
  [undef, 'host1', undef], 'just host');

  
isnt(pdb_master::parse_hostref(''), '', 'empty ref');
ok(not pdb_master::parse_hostref('user@:/path1'), 'missing host');
