use strict;
use warnings FATAL => 'all';
use Test::More tests => 10;
use File::Path qw(rmtree);
use Cwd 'abs_path';

BEGIN {
  require_ok('src/pdb-master.in.pl');
  mkdir('t_srcdir');
  mkdir('t_srcdir/data');
  for(qw(one two three)) {
    open my $fh, ">t_srcdir/data/$_" or die('Unable to create test data "'. $_ .'"');
    print $fh "$_\n";
    close($fh);
  }
  mkdir('t_dstdir');
}

END {
  rmtree('t_srcdir');
  rmtree('t_dstdir');
}

my $ssh_user = $ENV{'TEST_SSH_USER'} || $ENV{'LOGNAME'};
my $ssh_host = 'localhost';
my $ssh_key  = $ENV{'TEST_SSH_KEY'};

for(qw(one two three)) {
  ok(-f "t_srcdir/data/$_", "srcdata $_ exists");
}

my $r = pdb_master::copy_data(1, $ssh_host, $ssh_user, $ssh_key, abs_path('t_srcdir'), abs_path('t_dstdir'));

for(qw(one two three)) {
  ok(! -f "t_dstdir/data/$_", "dst data $_ does not exist with dryrun");
}

$r = pdb_master::copy_data(0, $ssh_host, $ssh_user, $ssh_key, abs_path('t_srcdir'), abs_path('t_dstdir'));

is($r, 0, 'copy_data returned ok');

for(qw(one two three)) {
  ok(-f "t_dstdir/data/$_", "dst data $_ does exist without dryrun");
}
