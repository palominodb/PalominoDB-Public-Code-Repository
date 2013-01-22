# 006_zrm_open_last.t
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

use Test::More tests => 4;
use ProcessLog;
use ZRMBackup;

my $pl = ProcessLog->null;
$pl->quiet(1);

my $bk1 = ZRMBackup->new($pl, "t/files/bk1");
ok($bk1, "parsed ok");
ok(!$bk1->open_last_backup, "fail to open non-existant backup");

my $bk3 = ZRMBackup->new($pl, "t/files/bk3");
ok($bk3, "parsed ok");
ok($bk3->open_last_backup, "opened bk2");
