# 012_inifile.t
# Copyright (C) 2013 PalominoDB, Inc.
# 
# You may contact the maintainers at eng@palominodb.com.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

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
