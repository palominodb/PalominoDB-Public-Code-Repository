#!/usr/bin/env perl
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
