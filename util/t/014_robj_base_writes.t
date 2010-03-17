use strict;
use warnings FATAL => 'all';
use Test::More;
use TestUtil;
use Fcntl qw(:seek);
use RObj::Base;
use File::Glob;

my $rb = RObj::Base->new;
my $fake_fh;
plan tests => 1;
open my $fh, '>', \$fake_fh;

eval { $rb->write_message( $fh, \*STDIN ); };
ok($@, 'die on invalid obj');

close($fh);
