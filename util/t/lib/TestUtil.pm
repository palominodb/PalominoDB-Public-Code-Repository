package TestUtil;
use strict;
use warnings FATAL => 'all';
use File::Basename;
use Exporter;
use File::Glob;

BEGIN {
  require Test::More;
  if($Test::More::VERSION < 0.94) {
    Test::More->import();
    sub new_ok {
      my ($class, $args) = @_;
      my $o = $class->new(@$args);
      isa_ok($o, $class);
      return $o;
    }
  }
  else {
    Test::More->import();
  }
}

use vars qw($VERSION @ISA @EXPORT);
$VERSION = 0.01;
@ISA = qw(Exporter);
@EXPORT = qw(get_test_dir get_files_dir get_test_data slurp new_ok fake_use);

our $PDB_CODE_ROOT = $ENV{PDB_CODE_ROOT} || die("PDB_CODE_ROOT not set to path to pdb-code checkout");

sub get_test_dir ($) {
  my $hide = shift || 0;
  my @cvars = caller($hide);
  my ($name, $path, $suf) = fileparse($cvars[1]);
  $path;
}

sub get_files_dir () {
  get_test_dir(1) . "files";
}

sub get_test_data ($;$) {
  my ($test_dir, $ext) = @_;
  $ext ||= 'txt';
  my @files = glob(get_test_dir(1) . "files" . "/$test_dir/*.$ext");
  return wantarray ? @files : scalar @files;
}

sub slurp ($) {
  my $file = shift;
  my $content;
  open(my $fh, '<', $file);
  { local $/; $content = <$fh>; }
  close($fh);
  $content;
}

sub fake_use {
  my ($filename) = @_;
  if (exists $INC{$filename}) {
    return 1 if $INC{$filename};
  }
  my $realfilename;
  ITER: {
    foreach my $prefix (@INC) {
      $realfilename = "$prefix/$filename";
      if (-f $realfilename) {
        $INC{$filename} = $realfilename;
        last ITER;
      }
    }
  }
  return 1;
}

1;
