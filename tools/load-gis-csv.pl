#!/usr/bin/env perl
# Loads data from a GIS csv dump in exactly the format seen below.
# Could be faster if done in multiple threads or something.
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
my $dbh = DBI->connect("DBI:mysql:host=localhost;database=tigerline", "root");
# 16 cols
my $sth = $dbh->prepare("INSERT INTO tigerline VALUES(?, ?, GeomFromText(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
while(<>) {
  my @cols = split "\t";
  my @new_cols = map { $_ eq "null" ? undef : $_ } @cols;
  print STDERR "loading: $cols[0]\n";
  $sth->execute(@new_cols);
}
