package Which;
use strict;
use warnings FATAL => 'all';
use Carp;

sub which($) {
  my $cmd = shift;
  croak "No command to which specified" if(!$cmd);
  # If $cmd starts with ./, /, or has / in it
  if( $cmd =~ /^\.?\// or $cmd =~ /\// ) {
    return $cmd if(-f $cmd and -x $cmd);
    return undef;
  }
  for(split(/:/, $ENV{'PATH'})) {
    return "$_/$cmd" if(-f "$_/$cmd" and -x "$_/$cmd");
  }
  return undef;
}

1;
