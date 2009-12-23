use Test::More tests => 2;
use English qw(-no_match_vars);
require_ok('Pdb::DSN');
ok(Pdb::DSN->new(), "instantiation check");
