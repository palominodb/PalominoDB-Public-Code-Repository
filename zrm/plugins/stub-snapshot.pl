#!/usr/bin/perl
# Copyright (c) 2010-2011 PalominoDB, Inc.  All Rights Reserved.
#
# Based on socket-copy.pl and socket-server.pl distributed in
# ZRM version 2.0 copyright Zmanda Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
#
# Contact information: PalominoDB Inc, 57 South Main St. #117
# Neptune, NJ 07753, United States, or: http://www.palominodb.com
#
#
# Stubbed out. Just enough for ZRM to believe it's calling a snapshot plugin.
#

use strict;
use warnings FATAL => 'all';
use lib '/usr/lib/mysql-zrm';
use ZRM::SnapshotCommon;

&initSnapshotPlugin();

exit(0);