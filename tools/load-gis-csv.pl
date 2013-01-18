#!/usr/bin/env perl
# load-gis-csv.pl - Loads data from a GIS csv dump in exactly the format seen below.
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
use warnings;
use DBI;

#CREATE TABLE `tigerline` (
#  `tlid` bigint(20) DEFAULT NULL,
#  `mtfcc` char(5) DEFAULT NULL,
#  `wkb_geometry` geometry NOT NULL,
#  `fullname` varchar(100) DEFAULT NULL,
#  `fromhnl` varchar(12) DEFAULT NULL,
#  `tohnl` varchar(12) DEFAULT NULL,
#  `fromhnr` varchar(12) DEFAULT NULL,
#  `tohnr` varchar(12) DEFAULT NULL,
#  `placefpr` char(5) DEFAULT NULL,
#  `placefpl` char(5) DEFAULT NULL,
#  `placer` varchar(60) DEFAULT NULL,
#  `placel` varchar(60) DEFAULT NULL,
#  `zipl` char(5) DEFAULT NULL,
#  `zipr` char(5) DEFAULT NULL,
#  `countyfp` char(3) DEFAULT NULL,
#  `statefp` char(2) DEFAULT NULL,
#  UNIQUE KEY `tlid` (`tlid`),
#  SPATIAL KEY `wkb_geometry` (`wkb_geometry`)
#) ENGINE=MyISAM DEFAULT CHARSET=latin1


# 24046141	S1400	LINESTRING(-93.868768 34.604785,-93.86891 34.60482,-93.86899 34.604874,-93.869168 34.605017,-93.86964 34.605275,-93.869867 34.605424,-93.870119 34.60555,-93.870659 34.605751,-93.870885 34.605878,-93.871042 34.606043,-93.871137 34.606233,-93.871254 34.606544,-93.871385 34.606769)	Otsu Ln	null	null	null	null	null	null	null	null	null	null	097	05

# Could be faster if done in multiple threads or something.
my $dbh = DBI->connect("DBI:mysql:host=localhost;database=tigerline", "root");
# 16 cols
my $sth = $dbh->prepare("INSERT INTO tigerline VALUES(?, ?, GeomFromText(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
while(<>) {
  my @cols = split "\t";
  my @new_cols = map { $_ eq "null" ? undef : $_ } @cols;
  print STDERR "loading: $cols[0]\n";
  $sth->execute(@new_cols);
}
