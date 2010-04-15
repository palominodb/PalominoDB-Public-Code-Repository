package ReadOnly;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Exporter;
use MysqlSlave;
use Carp;
use MysqlSlave;
use FailoverPlugin;
our @ISA = qw(FailoverPlugin);

sub pre_verification {
  my ($self, $pdsn, $fdsn) = @_;
  my $fslave = MysqlSlave->new($fdsn);
  $self->{read_only_var} = $fslave->read_only();
  if(!$self->{read_only_var}) {
    $::PLOG->i('Warning: read_only is NOT set to 1 on', $fdsn->get('h'));
    if(!$FailoverPlugin::force) {
      my $r = $::PLOG->p('Continue failover [Yes/No]:', qr/^(Yes|No)$/i); 
      if(lc($r) eq lc('No')) {
        croak('Aborting failover'); 
      }
    }
    else {
      $::PLOG->i('Warning: --force used, ignoring failure.');
    }
  }
  else {
    $::PLOG->m('read_only is set to 1 on', $fdsn->get('h'));
  }
}

sub post_verification {
  my ($self, $status, $pdsn, $fdsn) = @_;
  my $fslave = MysqlSlave->new($fdsn);
  if($self->{read_only_var} == $fslave->read_only()) {
    $::PLOG->i('Warning: read_only was not switched on', $fdsn->get('h'));
  }
  else {
    $::PLOG->m('read_only switched.');
  }
}

1;
