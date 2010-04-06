package DummyYAML;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Carp;
use FailoverPlugin;
use FailoverModule;
our @ISA = qw(FailoverModule);

our %failed_over = ();

sub options {
  return ( 'slave=s@' );
}

sub run {
  my ($self) = @_;
  unless($FailoverModule::pretend) {
    foreach my $sl (@{$self->{'slave'}}) {
      $failed_over{$sl} = 1;
    }
  }
  return 0;
}

1;
