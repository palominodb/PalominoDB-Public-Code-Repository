# 026_mysql_slave.t
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
use Test::More tests => 20;
use TestDB;
use DSN;
use TestUtil;

BEGIN {
  use_ok('MysqlSlave');
  my $tdb = TestDB->new();
  # Ensure that read_only is the way we expect
  eval { $tdb->dbh()->do('RESET MASTER'); };
  eval { $tdb->dbh()->do('RESET SLAVE'); };
  eval { $tdb->dbh()->do('STOP SLAVE'); };
  $tdb->dbh()->do('SET GLOBAL read_only = 0');
  $tdb->dbh()->do("CHANGE MASTER TO MASTER_HOST='fakehost', MASTER_USER='msandbox', MASTER_PASSWORD='msandbox', MASTER_LOG_FILE='fakehost-bin.000001', MASTER_LOG_POS=4");
  $tdb->dbh()->do('START SLAVE');
}

END {
  my $tdb = TestDB->new();
  $tdb->dbh()->do('RESET SLAVE');
  $tdb->dbh()->do("CHANGE MASTER TO MASTER_HOST=''");
}

my $tdb = TestDB->new();

## read_only

my $slave = new_ok('MysqlSlave' => [$tdb->{dsn}]);
is($slave->read_only(), 0, 'read_only false');
is($slave->read_only(1), 1, 'set read_only true');
is($slave->read_only(), 1, 'read_only true');
$slave->read_only(0);

eval {
  $slave->read_only('Y');
};
like($@, qr/value must be 0 or 1/, 'croaks with invalid input');

## auto_incrment

is($slave->auto_inc_inc(), 1, 'auto_inc_inc');
is($slave->auto_inc_off(), 1, 'auto_inc_off');

TODO: {
  local $TODO = 'Pending test framework updates';
  eval {
    $slave->read_only(1);
  };
  like($@, qr/denied.*super/i, 'set read_only croaks without SUPER priv');
};

## master_status

is($slave->master_status(), 0, 'non-master returns false in scalar');
is_deeply([$slave->master_status()], [undef, undef], 'non-master in list context');

TODO: {
  local $TODO = 'Pending test framework updates';
  is_deeply([$slave->master_status()], ['mysql-bin.000001', 106], 'master in list context');
  is($slave->master_status(), 1, 'master in scalar context');
};

## slave_status

is($slave->slave_status()->{'Master_Host'}, 'fakehost', 'slave_status Master_Host');

## start/stop slave

eval { is($slave->stop_slave(), '0E0', 'stop slave'); };
eval { is($slave->start_slave(), '0E0', 'start slave'); };

eval { $slave->stop_slave(); };
eval { is($slave->start_slave('mysql-bin.000002', 4), '0E0', 'start slave until'); };

TODO: {
  local $TODO = 'Pending test framework updates';
  eval {
    $slave->stop_slave();
  };
  like($@, qr/denied.*super/i, 'stop slave croaks without SUPER');
};

## change master to

$slave->change_master_to(
  master_host     => 'fakehost',
  master_log_file => 'fakehost-bin.000001',
  master_log_pos  => 4
);

eval {
  $slave->change_master_to($tdb->{dsn}, master_log_file => 'mysql-bin.000001', master_log_pos => 4);
};
is($@, '', 'change_master_to with dsn');

eval {
  $slave->change_master_to(
    rooster => 1
  );
};
like($@, qr/Invalid option rooster/, 'change master croaks on invalid option');

$slave->change_master_to({ master_host => 'fakehost2' });
is($tdb->dbh()->selectall_arrayref('SHOW SLAVE STATUS')->[0]->[1], 'fakehost2', 'change master with hashref');

## flush logs

$slave->flush_logs();
