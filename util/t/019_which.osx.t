use strict;
use warnings FATAL => 'all';
use Test::More tests => 4;

BEGIN {
  use_ok('Which');
}

SKIP: {
  skip 'Only relevant on OSX', 3 unless( -d "/System/Library/Frameworks" );
  is(Which::which('launchctl'), '/bin/launchctl', "finds launchctl");
  is(Which::which('/bin/bash'), '/bin/bash', "finds /bin/bash from absolute path");
  is(Which::which('not-real'), undef, "does not find not-real");
}
