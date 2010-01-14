package NagiosAlert;
# Highly simplistic perl module to send passive nagios alerts.
# Depends on the C tool 'send_nsca', though, it could be made to not do that.
# Ideally, existing work could be leveraged, but, installing perl modules is a pain.

use strict;
use warnings;

use lib qw(../../tools/data_mgmt);
use ProcessLog;

use constant OK       => 0;
use constant WARNING  => 1;
use constant CRITICAL => 2;
use constant UNKNOWN  => 3;

sub new {
  my ($class, $plog, $args) = @_;
  $args ||= {};
  $args->{send_nsca} ||= qx/which send_nsca/;
  $args->{nsca_cfg} ||= qq(/etc/nagios/nsca.cfg);
  $args->{nagios_host} ||= qq(nagios);
  $args->{plog} = $plog;

  bless $args, $class;
  return $args;
}

sub send {
  my ($self, $host, $service, $status, @rest) = @_;

  @rest = $self->_quote(@rest);
  my $res = { code => -1, output => '', evalerr => '' };
  my $cmd = qq#echo -e '$host\\t$service\\t$status\\t@rest' |#;
  $cmd   .= qq#$self->{send_nsca} -H $self->{nagios_host} -c $self->{nsca_cfg}#;
  $self->{plog}->d("Execing: ", $cmd);
  $res = $self->{plog}->xs($cmd);
  if(($res->{code} >> 8) != 0) {
    $self->{plog}->es("Error ($res->{code}) sending nagios alert.", "Output:",$res->{output});
  }
  return $res;
}

# Escape the naughty characters.
sub _quote {
  my ($self, @naughty) = @_;
  map {
    s/'/\\'/g;
    s/\n/\\n/g;
  } @naughty;
  return @naughty;
}

1;
