package Dummy;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Carp;
use FailoverPlugin;
use FailoverModule;
our @ISA = qw(FailoverModule);

our $failed_over = 0;
our $pretend;

sub new {
  my ($class, $pri_dsn, $fail_dsn, $opts) = @_;
  croak('Required flag --dummy missing') unless $opts->{'dummy'};
  return bless $class->SUPER::new($pri_dsn, $fail_dsn, $opts), $class;
}

sub options {
  return ( 'dummy' );
}

sub run {
  my ($self) = @_;
  my $pdsn = $self->{'primary_dsn'};
  my $fdsn = $self->{'failover_dsn'};
  FailoverPlugin->pre_verification_hook($pdsn, $fdsn);
  FailoverPlugin->begin_failover_hook($pdsn, $fdsn);

  $::PLOG->m('Dummy failover module.') unless($pretend);
  $failed_over = 1 unless($pretend);

  FailoverPlugin->finish_failover_hook(1, $pdsn, $fdsn);
  FailoverPlugin->post_verification_hook(1, $pdsn, $fdsn);
  return 0;
}

1;
