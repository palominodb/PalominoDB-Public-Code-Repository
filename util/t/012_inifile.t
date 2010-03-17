use strict;
use warnings FATAL => 'all';
use Test::More;
use TestUtil;
use File::Glob;
use IniFile;

my @tests = glob(get_files_dir() ."/inifile/*.txt");

plan tests => scalar(@tests);

TEST: for(@tests) {
  my $tf = $_;
  my %res = IniFile::read_config($tf);
  my $expected;
  {
    my $cnt;
    my $f;
    unless(open $f, "<$tf.exp") {
      diag("Failed to open expectation: $tf.exp");
      fail($tf);
      next TEST;
    }
    local $/;
    $cnt = <$f>;
    close($f);
    $expected = eval "$cnt";
    if($@) {
      diag('Expectation failed to parse correctly. Eval error: ', $@);
      fail($tf);
      next TEST;
    }
  };
  is_deeply(\%res, $expected, $tf);
}
