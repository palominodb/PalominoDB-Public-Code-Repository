package TestMod;
use strict;
use warnings FATAL => 'all';

sub new {
  return bless {}, $_[0];
}

sub ok {
  return 1;
}
