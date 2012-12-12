#!/usr/bin/perl
#
# A stub program to convince ZRM that it is calling a snapshot plug-in.
# Based on socket-copy.pl and socket-server.pl distributed in
# ZRM version 2.0 copyright Zmanda Inc.
#
# Copyright (C) 2010-2012 PalominoDB, Inc.
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
use lib '/usr/lib/mysql-zrm';
use ZRM::SnapshotCommon;

&initSnapshotPlugin();

exit(0);
