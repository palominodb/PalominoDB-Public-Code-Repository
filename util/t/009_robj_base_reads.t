use strict;
use warnings FATAL => 'all';
use Test::More;
use TestUtil;
use Fcntl qw(:seek);
use RObj::Base;
use File::Glob;

my $rb = RObj::Base->new;
my @tests = glob(get_files_dir() ."/robj/*.txt");
plan tests => scalar @tests;
for(@tests) {
  open(my $fh, '<', $_);
  my @r = $rb->read_message( $fh );
  isnt(@r, undef, $_);
  close($fh);
}

