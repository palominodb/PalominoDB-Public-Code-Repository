use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Test::More tests => 14;
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



