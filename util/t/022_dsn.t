use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Test::More tests => 6;
BEGIN {
  use_ok('DSN');
}
use TestDB;

my $p = DSNParser->default();
my $dsn = $p->parse($TestDB::dsnstr);
my $dsn2 = $p->parse("h=testhost");

is($dsn->get('u'), 'msandbox', 'user: msandbox');
is($dsn->get('p'), 'msandbox', 'pw: msandbox');
ok($dsn->has('h'), 'has host');

$dsn2->fill_in($dsn);
ok($dsn2->has('u'), 'fill_in sets new keys');
is($dsn2->get('h'), 'testhost', "fill_in does not overwrite keys");
