#!/usr/bin/perl
#
# Stubbed out. Just enough for ZRM to believe it's calling a snapshot plugin.
#

use strict;
use warnings FATAL => 'all';
use lib '/usr/lib/mysql-zrm';
use ZRM::SnapshotCommon;

&initSnapshotPlugin();

exit(0);