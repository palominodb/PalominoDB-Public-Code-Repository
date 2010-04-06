package TestMod2;
use strict;
use warnings FATAL => 'all';

sub frog {
  return bless {}, $_[0];
}

sub ok {
  return 1;
}
