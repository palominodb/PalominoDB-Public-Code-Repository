# 001_replication_lag_plugin.t
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
use TestUtil;
use TestDB;
use Test::More tests => 4;
use ProcessLog;
use Test::MockObject::Extends;
use FailoverPlugin;
use ReplicationLag;

BEGIN {
  my $pl = ProcessLog->new($0, 'pdb-test-harness');
  $pl = Test::MockObject::Extends->new($pl);
  no strict 'refs';
  *::PLOG = \$pl;
}

my $tdb = TestDB->new();
my $fp = ReplicationLag->new();
$fp = Test::MockObject::Extends->new($fp);

$fp->mock('get_lag', sub { return 0; });
eval {
  $fp->pre_verification($tdb->{dsn});
};

is($@, '', 'no croak with no lag');

$fp->mock('get_lag', sub { return 5; });
eval {
  $fp->pre_verification($tdb->{dsn});
};
like($@, qr/Replication lag/, 'croak with lag');

FailoverPlugin->global_opts('', 0, 1); # enable force
$::PLOG->mock('p', sub { return 'no' });
eval {
  $fp->pre_verification($tdb->{dsn});
};
like($@, qr/Replication lag/, 'croak with lag, force, and "no" response');
$::PLOG->mock('p', sub { return 'yes' });
eval {
  $fp->pre_verification($tdb->{dsn});
};
is($@, '', 'no croak with lag, force, and "yes" response');
