use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Test::More tests => 5;

BEGIN {
  use_ok('DSN');
}

my $p = DSNParser->new({});
eval {
  $p->parse('a=v');
};
like($EVAL_ERROR, qr/^Unknown key/, 'minimal/empty parser rejects everything');

$p = DSNParser->new({ 'h' => { 'mandatory' => 1, 'default' => '' } });

eval { $p->parse('h=localhost'); };
ok(!$EVAL_ERROR, 'simple/single-key parser accepts that key');

eval { $p->parse('h=localhost,u=naw'); };
like($EVAL_ERROR, qr/^Unknown key/, 'simple/single-key parser rejects other keys');

$p = DSNParser->new({
    'h' => { 'mandatory' => 1, 'default' => '' },
    'u' => { 'mandatory' => 0, 'default' => '' }
  });
eval { $p->parse('u=naw'); };
like($EVAL_ERROR, qr/^Missing key/, 'simple/double-key parser dies on missing mandatory keys');
