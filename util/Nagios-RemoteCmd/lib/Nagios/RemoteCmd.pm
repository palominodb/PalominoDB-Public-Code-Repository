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
$VERSION     = 0.10;
@ISA         = qw(Exporter);
@EXPORT      = ();
@EXPORT_OK   = ();
%EXPORT_TAGS = ();

=pod

=item new($url, $user, $password)

    url - URI to the root of your nagios install. E.g. http://nagios.example.com/nagios/
    user - username to authenticate with
    password - password for the user

=cut

sub new {
  my $class = shift;
  my ($nagios, $user, $pass) = @_;
  my $self = {};
  $self->{NAGIOS} = $nagios || undef;
  $self->{USER} = $user || undef;
  $self->{PASSWORD} = $pass || undef;
  $self->{LWP} = LWP::UserAgent->new;
  $self->{_DEBUG} = 0;
  bless $self, $class;
  return $self;
}

=pod

=item debug([$level])

Sets or gets the debug setting.

$level is an integer, but it's treated as a truth value for now.

=cut

sub debug {
 my ($self, $level) = @_;
 if(defined $level) {
   $self->{_DEBUG} = $level;
 }
 else {
  $self->{_DEBUG};
 }
}

sub _post {
  my ($self, $form) = @_;
  if($self->{_DEBUG} > 0) {
    print "Posting form to: ". $self->{NAGIOS} . "/cgi-bin/cmd.cgi\n";
    print Dumper($form);
  }
  my $r = POST($self->{NAGIOS} . "/cgi-bin/cmd.cgi", $form);
  $r->authorization_basic($self->{USER}, $self->{PASSWORD});
  my $resp = $self->{LWP}->request($r);

  if($self->{_DEBUG} > 0) {
    print "Got response: \n";
    print Dumper($resp);
  }
  $resp;
}

=pod

=item service_downtime($host, $service, $start_time, $length, $comment, $type)

Sets a downtime for $service on $host starting at $start_time, etc.

This function is hap-hazard at best right now. It's advised that yous use
disable/enable_notifications and have good error handling.

=cut

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

=pod

=item disable_notifications($host, $service)

Turns notifications off for $service on $host.

=cut

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

=pod

=item enable_notifications($host, $service)

Turns notifications back on for $service on $host.

=cut

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

=pod

=item add_comment($host, $service, $comment, $persistent, $author)

Adds $comment to $service on $host. If $persistent is a true value, then
the comment will persist between restarts of nagios.

Presently there is NO WAY to delete a comment from nagios.

=cut

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
__END__

=head1 NAME

Nagios::RemoteCmd - Do Nagios commands remotely.

=head1 SYNOPSIS

Nagios::RemoteCmd allows you to easily disable or enable notifications, schedule downtimes, and add comments to hosts and serivices.

No support for anything else right now.

