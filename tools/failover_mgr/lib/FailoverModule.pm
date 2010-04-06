package FailoverModule;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Carp;
use DSN;

our ($pretend, $force);

sub new {
  my ($class, $pri_dsn, $fail_dsn, $opts) = @_;
  $opts ||= {};
  my $self = bless $opts, $class;
  $self->{'primary_dsn'} = $pri_dsn;
  $self->{'failover_dsn'} = $fail_dsn;

  $::PLOG->d('Instantiating:', $self);
  return $self;
}

sub options { return () }

# Sets package variables corresponding to
# global options in FailoverManager package
sub global_opts {
  my $class = shift;
  ($pretend, $force) = @_;
}

sub DESTROY {
  my $self = shift;
  $::PLOG->d('Destroying:', $self);
}

sub run {
  croak("Cannot run FailoverModule base");
}

1;
