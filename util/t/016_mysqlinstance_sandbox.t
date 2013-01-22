# 016_mysqlinstance_sandbox.t
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
use Test::More tests => 7;
use TestUtil;
use MysqlInstance;

my $msb_path = $ENV{TEST_SANDBOX_PATH};
SKIP: {
  skip 'need TEST_SANDBOX_PATH setup', 7 unless($msb_path);
  diag('These tests can take a very long time. Be patient.');
  # This primes the sandbox
  # Since this tests needs to test 'start'
  system("$msb_path/stop");
  my $meths = MysqlInstance::Methods->new(
    "$msb_path/start 2>&1 | head -n1 | grep -vqE 'not|already'",
    "$msb_path/stop 2>&1 | wc -c | grep -q 0",
    "$msb_path/restart 2>&1 | head -n1 | grep -vqE 'not|already'",
    "$msb_path/status &>/dev/null",
    "$msb_path/my.sandbox.cnf"
  );
  my $l = MysqlInstance->new('localhost');
  $l->methods($meths);
  is($l->start, 0, 'start returns successfully');
  is($l->stop, 0, 'stop returns successfully');
  is($l->status, 1, 'status for stopped returns successfully');
  is($l->restart, 0, 'restart returns successfully');
  is($l->status, 0, 'status for started returns successfully');

  my $dbh = $l->get_dbh('msandbox', 'msandbox');
  is(ref($dbh), 'DBI::db', 'successfully get dbh');
  is($dbh->selectcol_arrayref('SELECT 1;')->[0], 1, 'can execute query');
  $dbh->disconnect;
}
