package ReplicationLag;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Exporter;
use Carp;
use DBI;
use FailoverPlugin;
our @ISA = qw(FailoverPlugin);

sub options {
  return ( 'hb_table=s', 'hb_col=s' );
}

sub get_lag {
  my $self = shift;
  my $dsn = shift;
  my $sql = 'SHOW SLAVE STATUS';
  my $col = 'Seconds_Behind_Master';

  if($self->{'hb_table'} and $self->{'hb_col'}) {
    my $hb_table = $self->{'hb_table'};
    my $hb_col = $self->{'hb_col'} || 'ts';
    $::PLOG->d('Using heartbeat table:', $self->{'heartbeat'});
    $sql = "SELECT NOW() - $hb_col FROM $hb_table";
    $col = $hb_col;
  }
  my $r = $dsn->get_dbh(1)->selectrow_hashref($sql, { Slice => {} });
  if(defined $r) {
    return $r->{$col};
  }
  return undef;
}

sub pre_verification {
  my ($self,@dsns) = @_;

  foreach my $dsn (@dsns) {
    my $lag = $self->get_lag($dsn);
    if(not defined($lag)) {
      $::PLOG->e('No replication, or replication not running.');
      croak('No replication, or replication not running') unless($FailoverPlugin::force)
    }
    $::PLOG->m($dsn->get('h'),'replication lag:', $lag);
    if($lag) { $::PLOG->e('Replication lag found!'); }
    if($lag and !$FailoverPlugin::force) {
      croak('Replication lag');
    }
    elsif($lag and $FailoverPlugin::force) {
      my $r = $::PLOG->p('Continue with lag [Yes/no]?',
        qr/^(Yes|No)$/i, 'Yes');
      if(lc($r) eq 'no') {
        croak('Replication lag');
      }
    }
  }

}

sub post_verification {
  my ($self, $status, @dsns) = @_;
  foreach my $dsn (@dsns) {
    my $lag = $self->get_lag($dsn);
    $::PLOG->m($dsn->get('h'), 'replication lag:', $lag);
  }
}

1;
