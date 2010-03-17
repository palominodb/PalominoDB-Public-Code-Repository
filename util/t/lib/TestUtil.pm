package TestUtil;
use strict;
use warnings FATAL => 'all';
use File::Basename;
use Exporter;

use vars qw($VERSION @ISA @EXPORT);
$VERSION = 0.01;
@ISA = qw(Exporter);
@EXPORT = qw(get_test_dir get_files_dir slurp);

sub get_test_dir ($) {
  my $hide = shift || 0;
  my @cvars = caller($hide);
  my ($name, $path, $suf) = fileparse($cvars[1]);
  $path;
}

sub get_files_dir () {
  get_test_dir(1) . "files";
}

sub slurp ($) {
  my $file = shift;
  my $content;
  open(my $fh, '<', $file);
  { local $/; $content = <$fh>; }
  close($fh);
  $content;
}

1;
