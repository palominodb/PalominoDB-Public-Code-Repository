# 017_mysqlmasterinfo.t
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
no strict 'refs';
use warnings FATAL => 'all';
use Test::More;
use TestUtil;
use File::Temp qw(tempfile);

my $ntests = get_test_data('mysqlmasterinfo');
plan tests => $ntests*2+3;
use_ok('MysqlMasterInfo');

my $once=1;
for(get_test_data('mysqlmasterinfo')) {
  my ($col, $exp) = ($_ =~ /([\w_]+)_is_(.*?)\.txt$/);
  my $mi = MysqlMasterInfo->open($_);
  isa_ok($mi, 'MysqlMasterInfo') if $once;
  is(&{"MysqlMasterInfo::$col"}($mi), $exp, "$col is $exp");
  my ($tfh, $tfile) = tempfile();
  close($tfh);
  if($once) {
    is($mi->write($tfile), 0, 'no write errors');
  }
  else {
    $mi->write($tfile);
  }

  my $tmi = MysqlMasterInfo->open($tfile);
  is(&{"MysqlMasterInfo::$col"}($tmi),
     &{"MysqlMasterInfo::$col"}($mi), "$col is written and read correctly");
  unlink($tfile);

  # Decrements to 0 on first iteration and disables
  # tests that only need to run once and we don't care
  # which datafile they do it with
  if($once > 0) { $once--; }
}
