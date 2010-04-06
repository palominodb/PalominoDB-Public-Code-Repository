package Plugin;
use strict;
use warnings FATAL => 'all';
use Carp;

sub load {
  my ($class, $method) = @_;
  $method ||= 'new';
  my $tries = 0;
RETRY:
  eval {
    unless($class->can($method)) {
      die("Class missing $method method");
    }
  };
  if($@ and $@ =~ /Class missing $method method/) {
    eval {
      require "$class.pm";
    };
    if($@ and $@ =~ /Can't locate $class.pm/) {
      return 0;
    }
    else {
      $tries++;
      unless($tries > 1) {
        goto RETRY;
      }
      else {
        return 0;
      }
    }
  }
  return 1;
}

=pod

=head1 NAME

Plugin - Load perl modules from disk, or use inlined package

=head1 USAGE

  Plugin::load('SomeModule');         # Returns true when found, or false.
  Plugin::load('SomeModule', 'meth'); # Tests for 'meth' instead of 'new'.

=cut

1;
