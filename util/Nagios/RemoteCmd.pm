package Nagios::RemoteCmd;
use strict;
use warnings;
use Exporter;
use HTTP::Request::Common;
use LWP::UserAgent;
use URI;
use DateTime;
use Data::Dumper;

use constant SVC_DOWNTIME_FLEXIBLE => 0;
use constant SVC_DOWNTIME_FIXED    => 1;

use constant DISABLE_HOST_NOTIFICATIONS => 25;
use constant ENABLE_HOST_NOTIFICATIONS => 24;

use constant DISABLE_HOST_SVC_NOTIFICATIONS => 29;
use constant ENABLE_HOST_SVC_NOTIFICATIONS => 28;

use constant DISABLE_SVC_NOTIFICATIONS => 23;
use constant ENABLE_SVC_NOTIFICATIONS => 22;

use constant SCHEDULE_SVC_DOWNTIME  => 56;

use constant HOST_COMMENT => 1;
use constant SVC_COMMENT => 3;

use constant CMD_MOD   => 2; # Means 'go', I think.

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);
$VERSION     = 0.00;
@ISA         = qw(Exporter);
@EXPORT      = ();
@EXPORT_OK   = ();
%EXPORT_TAGS = ();


sub new {
  my $class = shift;
  my ($nagios, $user, $pass) = @_;
  my $self = {};
  $self->{NAGIOS} = $nagios || undef;
  $self->{USER} = $user || undef;
  $self->{PASSWORD} = $pass || undef;
  $self->{LWP} = LWP::UserAgent->new;
  bless $self, $class;
  return $self;
}

sub _post {
  my ($self, $form) = @_;
  my $r = POST($self->{NAGIOS} . "/cgi-bin/cmd.cgi", $form);
  $r->authorization_basic($self->{USER}, $self->{PASSWORD});
  $self->{LWP}->request($r);
}

sub service_downtime {
  my ($self, $host, $service, $start_time, $length, $comment, $type) = @_;
  my ($hours, $minutes) = split /\./, $length;
  my $form = [
    cmd_typ => SCHEDULE_SVC_DOWNTIME,
    cmd_mod => CMD_MOD,
    host    => $host,
    service => $service,
    com_author => $self->{USER},
    com_data   => $comment,
    trigger => 0, # TODO: Learn what this is and expose?
    fixed => $type,
    start_time => DateTime->now(time_zone => 'local')->strftime("%m-%d-%Y %T"),
    end_time => DateTime->now(time_zone => 'local')->add( hours => $hours, minutes => $minutes)->strftime("%m-%d-%Y %T")
    ];
  $self->_post($form);
}

sub disable_notifications {
  my ($self, $host, $service, $comment, $author) = @_;
  my $form = undef;
  if(defined $service) {
    $form = [
      cmd_typ => DISABLE_SVC_NOTIFICATIONS,
      cmd_mod => CMD_MOD,
      host => $host,
      service => $service
    ];
  }
  else {
    $form = [
      cmd_typ => DISABLE_HOST_SVC_NOTIFICATIONS,
      cmd_mod => CMD_MOD,
      host => $host
    ];
  } 
  $self->_post($form);

  if(defined $comment) {
    $self->add_comment($host, $service, $comment)
  }
}

sub enable_notifications {
  my ($self, $host, $service) = @_;
  my $form = undef;
  if(defined $service) {
    $form = [
      cmd_typ => ENABLE_SVC_NOTIFICATIONS,
      cmd_mod => CMD_MOD,
      host => $host,
      service => $service
    ];
  }
  else {
    $form = [
      cmd_typ => ENABLE_HOST_SVC_NOTIFICATIONS,
      cmd_mod => CMD_MOD,
      host => $host
    ];
  } 
  $self->_post($form);

}

sub add_comment {
  my ($self, $host, $service, $comment, $persistent, $author) = @_;
  my $form = {
    cmd_typ => defined $service ? SVC_COMMENT : HOST_COMMENT,
    cmd_mod => CMD_MOD,
    host => $host,
    service => $service,
    persistent => '',
    com_author => defined $author ? $author : $self->{USER},
    com_data => $comment
  };
  if(not defined $persistent) {
    delete $form->{'persistent'}
  }
  $self->_post($form);
}

1;
