use strict;
use warnings FATAL => 'all';
use TestDB;
use TestUtil;
use Test::More tests => 6;

my @COMMON_ARGS = ('--logfile=pdb-test-harness');

require_ok('src/pdb-munch.in.pl');
is(pdb_munch::main(@COMMON_ARGS, '--dump-spec'), 0, '--dump-spec returns 0');
ok(-f "default_spec.conf", "default_spec.conf exists");
is(pdb_munch::main(@COMMON_ARGS), 1, 'missing --spec returns 1');
is(pdb_munch::main(@COMMON_ARGS, '--spec=default_spec.conf'), 1, 'missing --config returns 1');
open my $fh, ">test.conf";
print $fh <<EOF;
[__connection__]
dsn = $TestDB::dsnstr,D=munch

[foo]
c1 = name
c2 = value
EOF
close($fh);

push @COMMON_ARGS, ('--spec=default_spec.conf', '--config=test.conf');

is(pdb_munch::main(@COMMON_ARGS), 1, 'missing spec source handled');

END {
  unlink('default_spec.conf');
  unlink('test.conf');
  unlink('t/pdb-munch/basic.t.log');
}
