#!/usr/bin/perl -w
$| = 1;

use strict;

use Data::Dumper;
use Storable;

my $cache = retrieve($ARGV[0]);

print Dumper($cache);
