use strict;
use warnings FATAL => 'all';
use Test::More tests => 5;
use Data::Dumper;
use TestUtil;
BEGIN {
  use_ok('RObj::Base');
}
use Fcntl qw(:seek);

my ($rb, $fh, $content, @r);
$rb = new_ok( 'RObj::Base' );

open($fh, "+>", undef);
$rb->write_message( $fh, ["test1", "test2"] );
seek($fh, 0, SEEK_SET);
sysread($fh, $content, 10240);
is($content, 'BAcIMTIzNDU2NzgECAgIAgEAAAAEAgIAAAAKBXRlc3QxCgV0ZXN0Mg==
30d575944d8f58807501fcd6604d53f81352fb52
ok
', 'test array serialized');

seek($fh, 0, SEEK_SET);
@r = $rb->read_message( $fh );
is_deeply(\@r, [ ["test1", "test2"] ], "test array deserialized");


$rb->write_message( $fh, { test3 => 'test4' } );
seek($fh, 0, SEEK_SET);
@r = $rb->read_message( $fh );
is_deeply(\@r, [ ['test1', 'test2'], { test3 => 'test4' } ], "can parse many messages");
close($fh);
