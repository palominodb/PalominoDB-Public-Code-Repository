package DummyComposite;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Carp;
use FailoverPlugin;
use FailoverModule;
use Dummy;
our @ISA = qw(FailoverModule);

our $failed_over = 0;
our $pretend;

sub new {
  my ($class, $pri_dsn, $fail_dsn, $opts) = @_;
  croak('Required flag --dummy-composite missing') unless $opts->{'dummy-composite'};
  return bless $class->SUPER::new($pri_dsn, $fail_dsn, $opts), $class;
}

sub options {
  return ( 'dummy-composite' );
}

sub run {
  my ($self) = @_;
  my $pdsn = $self->{'primary_dsn'};
  my $fdsn = $self->{'failover_dsn'};
  FailoverPlugin->pre_verification_hook($pdsn, $fdsn);
  FailoverPlugin->begin_failover_hook($pdsn, $fdsn);

  {
    local $FailoverPlugin::no_hooks = 1;
    my $dm = Dummy->new($pdsn, $fdsn, { 'dummy' => 1 });
    $::PLOG->m('Dummy composite failover module.') unless($pretend);
    $dm->run();
    $failed_over = 1 unless($pretend);
  };

  FailoverPlugin->finish_failover_hook(1, $pdsn, $fdsn);
  FailoverPlugin->post_verification_hook(1, $pdsn, $fdsn);
  return 0;
}

1;
