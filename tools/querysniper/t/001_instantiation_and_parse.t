use strict;
use warnings FATAL => 'all';
use Test::More tests => 3;
use QuerySniper;

new_ok('QueryRules');
my $qr = QueryRules->new;
ok($qr->load('t/all_pass.rules'), 'bare minimum');
eval {
  $qr->load('t/empty.rules');
};
ok($@, 'empty rules') || diag('Empty rules should never pass! Something is very wrong.');
