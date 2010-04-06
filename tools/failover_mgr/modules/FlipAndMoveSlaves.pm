package FlipAndMoveSlaves;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Carp;
use DSN;
use FailoverPlugin;
use FailoverModule;
use FlipReadOnly;
use MoveSlaves;
our @ISA = qw(FailoverModule);

our $pretend;

sub new {
  my ($class, $pri_dsn, $fail_dsn, $opts) = @_;
  my $self = bless $class->SUPER::new($pri_dsn, $fail_dsn, $opts), $class;
  croak('Required flag --slave missing') unless $opts->{'slave'};
  my $dsnp = DSNParser->default();
  @{$self->{'slave'}} = map { if(ref($_) and ref($_) eq 'DSN') { $_; } else { $_ = $dsnp->parse($_); $_->fill_in($pri_dsn); } } @{$self->{'slave'}};
  return $self;
}

sub options {
  return ( 'slave|s=s@' );
}

sub run {
  my ($self) = @_;

  my $pdsn = $self->{'primary_dsn'};
  my $fdsn = $self->{'failover_dsn'};
  my @slaves = @{$self->{'slave'}};

  FailoverPlugin->pre_verification_hook($pdsn, $fdsn, @{$self->{'slave'}});
  FailoverPlugin->begin_failover_hook($pdsn, $fdsn, @{$self->{'slave'}});

  my $flipRO = FlipReadOnly->new($pdsn, $fdsn);
  my $moveSlaves = MoveSlaves->new($pdsn, $fdsn, { 'slave' => $self->{'slave'} });
  eval {
    {
      # Make sure other failover modules don't run hooks.
      local $FailoverPlugin::no_hooks = 1;
      $flipRO->run();
      $moveSlaves->run();
    };
  };

  if($@) {
    $::PLOG->e(__PACKAGE__, 'failover FAILED');
    $::PLOG->e('Got error:', $@);
    FailoverPlugin->finish_failover_hook(0, $pdsn, $fdsn, @{$self->{'slave'}});
    FailoverPlugin->post_verification_hook(0, $pdsn, $fdsn, @{$self->{'slave'}});
    croak($@);
  }
  FailoverPlugin->finish_failover_hook(1, $pdsn, $fdsn, @{$self->{'slave'}});
  FailoverPlugin->post_verification_hook(1, $pdsn, $fdsn, @{$self->{'slave'}});

  return 0;
}

1;
