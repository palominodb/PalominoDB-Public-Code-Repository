# 022_dsn.t
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
use English qw(-no_match_vars);
use Test::More tests => 17;
BEGIN {
  use_ok('DSN');
}
use TestDB;

my $p = DSNParser->default();
my $dsn = $p->parse($TestDB::dsnstr);
my $dsn2 = $p->parse("h=testhost");

is($dsn->get('u'), 'root', 'user: root');
is($dsn->get('p'), 'msandbox', 'pw: msandbox');
ok($dsn->has('h'), 'has host');
is($dsn->str(), "P=$TestDB::port,S=$TestDB::socket,h=localhost,p=msandbox,u=root", "str() reconstructs properly");
is($dsn->get_dbi_str(), "DBI:mysql:port=$TestDB::port;mysql_socket=$TestDB::socket;host=localhost;", "get_dbi_str()");

$dsn2->fill_in($dsn);
ok($dsn2->has('u'), 'fill_in sets new keys');
is($dsn2->get('h'), 'testhost', "fill_in does not overwrite keys");

my $dsn3 = $p->parse("h=localhost,P=$TestDB::port,S=$TestDB::socket,u=invalid,p=bad");

eval {
  my $dbh = $dsn3->get_dbh();
};
like($@, qr/Access denied/, "get_dbh dies for Access denied");

my $dsn4 = $p->parse('h=localhost');
$dsn4->{'vI'}->{'value'} = ['192.168.5.30'];
eval {
  $dsn->fill_in($dsn4);
};
is($@, '', 'array-ref values cause fatal warning');

my $p2 = DSNParser->new({ 'h' => { 'desc' => 'hostname', 'default' => '', 'mandatory' => 1 } });
eval {
  my $dsn5 = $p2->parse('');
};
diag($@);
like($@, qr/Missing key:/, 'missing key generates exception');

$p2 = DSNParser->new({ 'h' => { 'default' => '', 'mandatory' => 1 } });
eval {
  my $dsn5 = $p2->parse('');
};
diag($@);
like($@, qr/Missing key:/, 'missing description generates "Missing key:" exception');

$dsn4 = $p->parse('h=remote-box,SSL_key=/path/to/key,SSL_cipher=AES');
diag($dsn4->get_dbi_str());
like($dsn4->get_dbi_str(), qr/mysql_ssl=1;/, 'Adding an SSL option includes mysql_ssl option in DBI string');
unlike($dsn4->get_dbi_str(), qr/^\Gmysql_ssl=1;/, 'Does not add mysql_ssl more than once');

$dsn = $p->parse($TestDB::dsnstr);

eval {
  my $dbh = $dsn->get_dbh(1);
  my $r = $dbh->selectall_arrayref("SHOW TABLE STATUS FROM `fakedb` LIKE 'faketable'");
};
diag($@);
like($@, qr/Unknown database/, 'exception contains info');

is(
	$dsn4->get_dbi_str({'mysql_local_infile' => 1}),
	'DBI:mysql:mysql_ssl=1;mysql_ssl_cipher=AES;mysql_ssl_client_key=/path/to/key;host=remote-box;mysql_local_infile=1',
	'get_dbi_str supports extra option'
);

is(
	$dsn4->get_dbi_str({'mysql_local_infile' => 1, 'mysql_use_result' => 1}),
	'DBI:mysql:mysql_ssl=1;mysql_ssl_cipher=AES;'
	.'mysql_ssl_client_key=/path/to/key;host=remote-box;'
	.'mysql_local_infile=1;mysql_use_result=1',
	'get_dbi_str supports extra options'
);



