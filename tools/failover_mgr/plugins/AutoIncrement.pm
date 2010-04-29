package AutoIncrement;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Exporter;
use MysqlSlave;
use Carp;
use FailoverPlugin;
our @ISA = qw(FailoverPlugin);

sub pre_verification {
  my ($self, $pri_dsn, $fail_dsn) = @_;

  my $pri_s = MysqlSlave->new($pri_dsn);
  my $fail_s = MysqlSlave->new($fail_dsn);
  if($pri_s->auto_inc_off() == $fail_s->auto_inc_off()) {
    $::PLOG->e($pri_dsn->get('h'), 'auto_increment_offset:', $pri_s->auto_inc_off());
    $::PLOG->e($fail_dsn->get('h'), 'auto_increment_offset:', $fail_s->auto_inc_off());
    if($FailoverPlugin::force) {
      $::PLOG->i('Continuing due to --force being passed.');
    }
    croak('Failed pre-verification check: auto_increment_offset') unless($FailoverPlugin::force);
  }
  else {
    $::PLOG->m($pri_dsn->get('h'), 'auto_increment_offset:', $pri_s->auto_inc_off());
    $::PLOG->m($fail_dsn->get('h'), 'auto_increment_offset:', $fail_s->auto_inc_off());
  }
}

1;
