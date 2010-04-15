package FlipReadOnly;
use strict;
use warnings FATAL => 'all';
use Carp;
use DBI;
use ProcessLog;
use DSN;
use FailoverPlugin;
use FailoverModule;
our @ISA = qw(FailoverModule);

our $pretend;

sub run {
  my ($self) = @_;
  my $pdsn = $self->{'primary_dsn'};
  my $fdsn = $self->{'failover_dsn'};
  my $fdbh;
  my $status = 1;
  FailoverPlugin->pre_verification_hook($pdsn, $fdsn);
  FailoverPlugin->begin_failover_hook($pdsn, $fdsn);

  $::PLOG->m('Connecting to failover master.');
  eval {
    $fdbh = $fdsn->get_dbh(1);
  };
  if($@) {
    $::PLOG->e('Failover FAILED.');
    $::PLOG->e('DBI threw:', $@);
    FailoverPlugin->finish_failover_hook(0, $pdsn, $fdsn);
    FailoverPlugin->post_verification_hook(0, $pdsn, $fdsn);
    croak($@);
  }

  $::PLOG->m('Setting read_only=0');

  eval {
    $fdbh->do('SET GLOBAL read_only=0') unless($FailoverModule::pretend);
  };
  if($@) {
    $::PLOG->e('Failover FAILED.');
    $::PLOG->e('DBI threw:', $@);
    FailoverPlugin->finish_failover_hook(0, $pdsn, $fdsn);
    FailoverPlugin->post_verification_hook(0, $pdsn, $fdsn);
    croak($@);
  }

  FailoverPlugin->finish_failover_hook(1, $pdsn, $fdsn);
  FailoverPlugin->post_verification_hook(1, $pdsn, $fdsn);

  return 0;
}

1;
