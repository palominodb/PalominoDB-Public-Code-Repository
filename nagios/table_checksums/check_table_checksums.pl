#!/usr/bin/env perl
# check_table_checksums.pl - Check checksums on MySQL database tables
# within a time interval, and that the results are the same and not empty.
# Copyright (C) 2012 PalominoDB, Inc.
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
use Getopt::Long qw(:config no_ignore_case);
use English qw(no_match_vars);
use List::MoreUtils qw(uniq);
use Data::Dumper;
use DBI;

use constant OK       => 0;
use constant WARNING  => 1;
use constant CRITICAL => 2;
use constant UNKNOWN  => 2;

my $help = 0;
my $csum_host = 'localhost';
my $csum_user = 'nagios';
my $csum_pw   = 'n4g10s';
my $csum_table = 'palomino.checksums';
my $csum_sock = '';
my $csum_interval = 1;

# If 1, then alerts are critical, instead of warning.
my $alert_crit = 0;

my $alert_str = '';

my $dbh = undef;

GetOptions(
  'help|h' => \$help,
  'host|H=s' => \$csum_host,
  'user|u=s' => \$csum_user,
  'password|p=s' => \$csum_pw,
  'table|T=s'  => \$csum_table,
  'socket|S=s' => \$csum_sock,
  'critical|c' => \$alert_crit,
  'interval|I=i' => \$csum_interval
);

if($help) {
  print <<HELP_EOF;
Usage:
  check_table_checksums.pl -T $csum_table [-u $csum_user] [-p $csum_pw]

Options:
  --help,-h      This message.
  --host,-H      Set db host holding checksums. Default: $csum_host.
  --user,-u      Set db user Default: $csum_user.
  --password,-p  Set db password. Default: $csum_pw.
  --table,-T     Set table checksums are in. Default: $csum_table.
  --socket,-S    Set path to mysql socket, if needed.
  --critical,-c  Make alerts critical instead of warning.
  --interval,-I  How often checksums should be running in hours.
                 Default: ${csum_interval}h

Operation:
  This plugin ensures two things:
    a) There are results within the last ${csum_interval} hours.
    b) The checksums for those results are the same and not empty.
       That is, if the checksum for the master and slave are NULL,
       then that is considered an error, since it means no rows
       were checksummed.

HELP_EOF
  exit(UNKNOWN);
}

$alert_str = $alert_crit ? 'CRITICAL:' : 'WARNING:';

if($csum_sock ne '') {
  $csum_sock = ";mysql_socket=$csum_sock";
}

eval {
  $dbh = DBI->connect("DBI:mysql:host=$csum_host$csum_sock",
    $csum_user, $csum_pw,
    { AutoCommit => 0,
      RaiseError => 1,
      PrintError => 0,
      ShowErrorStatement => 1
    });
};
if($EVAL_ERROR) {
  chomp($EVAL_ERROR);
  print "CRITICAL: Error on connect: $EVAL_ERROR";
  exit(CRITICAL);
}

my $time_sql = <<EOFT;
  SELECT host, db, tbl FROM $csum_table
  WHERE ts >= NOW() - INTERVAL $csum_interval HOUR
  LIMIT 1
EOFT

my $diff_sql = <<EOFD;
   SELECT host, db, tbl, chunk, boundaries,
      COALESCE(this_cnt-master_cnt, 0) AS cnt_diff,
      COALESCE(
         this_crc <> master_crc OR ISNULL(master_crc) <> ISNULL(this_crc),
         0
      ) AS crc_diff,
      this_cnt, master_cnt, this_crc, master_crc
   FROM $csum_table
   WHERE ts >= NOW() - INTERVAL $csum_interval HOUR
   AND (
      master_cnt <> this_cnt OR master_crc <> this_crc
      OR ISNULL(master_crc) <> ISNULL(this_crc)
   )
EOFD

my $time_r = undef;
my $diff_r = undef;
eval {
  $time_r = $dbh->selectall_arrayref($time_sql, { Slice => {} });
  $diff_r = $dbh->selectall_arrayref($diff_sql, { Slice => {} });
};
if($EVAL_ERROR) {
  $dbh->disconnect;
  chomp($EVAL_ERROR);
  print "CRITICAL: Error while collecting data: $EVAL_ERROR";
  exit(CRITICAL);
}
$dbh->disconnect;

unless(scalar @$time_r) {
  print "$alert_str did not find recent checksum results.";
  exit($alert_crit ? CRITICAL : WARNING);
}

if(scalar @$diff_r) {
  my $last_host = '';
  my $tbls = join(',', uniq(map {
      $_->{'db'} . '.' . $_->{'tbl'}
    } @$diff_r));
  print "$alert_str found crc differences: $tbls\n\n";
  print "CHUNK TBL THIS_CRC MASTER_CRC THIS_CNT/MASTER_CNT\n";
  foreach my $tbl (@$diff_r) {
    print('On: ', $tbl->{'host'}, "\n") if $last_host ne $tbl->{'host'};
    print '  ', 'c', $tbl->{'chunk'}, ' ', $tbl->{'db'}, '.', $tbl->{'tbl'},
          ' ', $tbl->{'this_crc'} ? $tbl->{'this_crc'} : 'NULL',
          ' ', $tbl->{'master_crc'} ? $tbl->{'master_crc'} : 'NULL',
          ' ', $tbl->{'this_cnt'}, '/',
          $tbl->{'master_cnt'} ? $tbl->{'master_cnt'} : '0',
          (!$tbl->{'master_crc'} and !$tbl->{'this_crc'} ? ' (empty crc)' : ''),
          "\n";
  }
  exit($alert_crit ? CRITICAL : WARNING);
}
else {
  print "OK: no crc differences in the last $csum_interval hours";
  exit(OK);
}

print "UNKNOWN: Supposedly impossible conditions in plugin.";
exit(UNKNOWN);
