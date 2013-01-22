# 005_zrm_backup_autokeys.t
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

use Test::More tests => 24;
use ProcessLog;
use ZRMBackup;

my $pl = ProcessLog->null;
$pl->quiet(1);

my $bk2 = ZRMBackup->new($pl, "t/files/bk2");
ok($bk2, "parsed ok");
is($bk2->backup_set, "c2", "backup-set is c2");
is($bk2->backup_date, "20100226124502", "backup-date is 20100226124502");
is($bk2->mysql_server_os, "Linux/Unix", "mysql-server-os is Linux/Unix");
is($bk2->host, "c2s", "host is c2s");
is($bk2->backup_date_epoch, "1267217102", "backup-date-epoch is 1267217102");
is($bk2->retention_policy, "2W", "retention-policy is 2W");
is($bk2->mysql_zrm_version, "ZRM for MySQL Community Edition - version 2.0", "mysql-zrm-version is ZRM for MySQL Community Edition - version 2.0");
is($bk2->mysql_version, "5.0.84-percona-highperf-b18-log", "mysql-version is 5.0.84-percona-highperf-b18-log");
is($bk2->backup_directory, "/bk1/backups/c2/20100226124502", "backup-directory is /bk1/backups/c2/20100226124502");
is($bk2->backup_level, 1, "backup-level is 1");
is($bk2->incremental, "mysql-bin.[0-9]*", "incremental is mysql-bin.[0-9]*");
is($bk2->next_binlog, "mysql-bin.031414", "next-binlog is mysql-bin.031414");
is($bk2->last_backup, "/bk1/backups/c2/20100226084502", "last-backup is /bk1/backups/c2/20100226084502");
is($bk2->backup_size, 634480.64, "backup-size is 619.61 MB");
is($bk2->compress, "/usr/local/bin/gzip_fast.sh", "compress is /usr/local/bin/gzip_fast.sh");
is($bk2->backup_size_compressed, 111001.6, "backup-size-compressed is 108.40 MB");
is($bk2->read_locks_time, 0, "read-locks-time is 00:00:00");
is($bk2->flush_logs_time, 0, "flush-logs-time is 00:00:00");
is($bk2->compress_encrypt_time, 200, "compress-encrypt-time is 00:03:20");
is($bk2->backup_time, 68, "backup-time is 00:01:08");
is($bk2->backup_status, 1, "backup-status is Backup succeeded");

my $bk1 = ZRMBackup->new($pl, "t/files/bk1");
ok($bk1, "parsed ok");
is_deeply($bk1->replication, ['master.info', 'relay-log.info'], "replication ('master.info', 'relay-log.info')");
