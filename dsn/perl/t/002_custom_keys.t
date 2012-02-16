use Test::More tests => 5;
use English qw(-no_match_vars);

use Pdb::DSN;

my $dsn = Pdb::DSN->new();

eval {
  $dsn->open('t/files/custom_keys_dsn.yml');
};

ok(!$EVAL_ERROR, 'open dsn');

eval {
  is($dsn->server_checksum('s1'), 1, 'checksum key is y');
};

ok(!$EVAL_ERROR, 'server custom key');

eval {
  is($dsn->cluster_backup('c1'), 1, 'backup key is y');
};

ok(!$EVAL_ERROR, 'cluster custom key');
