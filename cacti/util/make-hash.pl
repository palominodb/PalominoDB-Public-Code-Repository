#!/usr/bin/env perl
# make-hash.pl - generate and output hashes from input values
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
use warnings FATAL=>'all';

use Digest::MD5 qw(md5_hex);
use Time::HiRes qw(gettimeofday);

sub usage {
  print "Usage: $0 <script name> [count] [extra]\n";
  print "When count specified, generate [count] hashes (Default: 1).\n";
  print "When extra specified, throw that into the to be hashed text (Default: '').\n";
}

unless( scalar @ARGV >= 1 ) {
  usage();
  exit(1);
}

my ($script, $type, $count, $extra) = @ARGV;
unless($script) {
  usage(); exit(1);
}
unless($type) {
  $type = 'XX',
}
unless($count) {
  $count = 1;
}
unless($extra) {
  $extra = "";
}

for(my $i=0; $i < $count; $i++) {
  print "hash_${type}_VER_", md5_hex("pdb-". $script ."-". gettimeofday() ."-". rand() ."-" . $extra), "\n";
}
