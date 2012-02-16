use Test::More tests => 4;
use English qw(-no_match_vars);
use Time::HiRes qw(usleep);
use Pdb::DSN;

system("test ! -f t/files/http.pid && lighttpd -D -f t/files/http.conf &");

my $dsn = Pdb::DSN->new();

eval {
  $dsn->open('t/files/local_dsn.yml');
};

ok(!$EVAL_ERROR, 'open local');

eval {
  $dsn->open('t/files/missing_dsn.yml');
};

ok($EVAL_ERROR, 'missing open local');

SKIP: {
  usleep(50000); # Give lighttpd a chance to come up.
  skip("Missing lighttpd", 2) unless(-f 't/files/http.pid');
  ok($dsn->open('http://localhost:9999/local_dsn.yml'), 'remote open');
  ok(!$dsn->open('http://localhost:9999/missing_dsn.yml'), 'missing remote open');
}

system("test -f t/files/http.pid && kill `cat t/files/http.pid 2>/dev/null` 2>/dev/null");
