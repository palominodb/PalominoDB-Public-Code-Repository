use strict;
use warnings FATAL => 'all';
use Test::More tests => 4;

BEGIN {
  use_ok('Which');
}

SKIP: {
  skip 'Only relevant on debian', 2 unless( -f "/etc/debian_version" );
  is(Which::which('dpkg'), '/usr/bin/dpkg', "finds dpkg");
  is(Which::which('not-real'), undef, "does not find not-real");
  is(Which::which('/bin/bash'), '/bin/bash', "finds /bin/bash from absolute path");
}
