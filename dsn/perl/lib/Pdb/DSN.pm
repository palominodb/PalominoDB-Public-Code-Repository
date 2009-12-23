package Pdb::DSN;
use strict;
use warnings FATAL => 'all';
use 5.008;
use Exporter;
use Error;

use HTTP::Request::Common;
use LWP::UserAgent;
use URI;

use YAML::Syck;
$YAML::Syck::ImplicitTyping = 1;

use Data::Dumper;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);
$VERSION     = 0.01;
@ISA         = qw(Exporter);
@EXPORT      = ();
@EXPORT_OK   = ();
%EXPORT_TAGS = ();

our $AUTOLOAD;

=pod

=item new($uri)

    uri: Location of the DSN file. May be local, or remote over HTTP.
    
    See further information at L<"open($uri)">.

=cut

sub new {
  my ($class, $uri) = @_;
  my $self = {
    uri => $uri,
    raw => undef,
    lwp => LWP::UserAgent->new,
  };

  bless $self, $class;
  if($uri) {
    $self->open($uri);
  }

  return $self;
}

=pod

=item open($uri)

    uri: Location of DSN file. May be local, or remote over HTTP.

    The decision process for http vs. local is very simple. If the uri
    starts with C<http://>, then it is accessed over HTTP. In any other
    case local access will be used.

=cut

sub open {
  my ($self, $uri) = @_;
  my $r=0;
  if($uri =~ /^http:\/\//) {
    $r=$self->_open_http($uri);
  }
  else {
    $r=$self->_open_local($uri);
  }
  return $r;
}

sub _open_http {
  my ($self, $uri) = @_;
  my $r = GET($uri);
  my $res = $self->{lwp}->request($r);
  $self->{raw} = Load($res);
  $self->validate;
  return $res->is_success == 1;
}

sub _open_local {
  my ($self, $uri) = @_;
  $self->{raw} = LoadFile($uri);
  $self->validate;
  return defined $self->{raw};
}

=pod

=item from_hash($hashref)

    hashref: Reference to a hash conforming to the DSN specification.

    It's strongly recommended to run validate after calling this.

=cut

sub from_hash {
  my ($self, $args) = @_;
  $self->{raw} = $args;
  return 1;
}

=pod

=item validate()

    This method, B<when implemented>, will validate that the DSN conforms to all
    requirements.

=cut

sub validate {
  my $self = shift;
  return 1;
}

=pod

=item get_write_hosts($cluster)

    Retrieve destinations for writes for C<$cluster>.

    This method returns all active servers which specify C<$cluster> in their writefor key.
    The way in which writes are load-balaned is application dependent. In general,
    only one database should take writes at any given time.

    See also: L<get_primary($cluster)>.

=cut

sub get_write_hosts {
  my ($self, $cluster) = @_;
  my @write_hosts = ();
  foreach my $srv (keys %{$self->{raw}->{'servers'}}) {
  }
}

=pod

=item get_read_hosts($cluster)

    Retrieve destinations for reads for C<$cluster>.

    This method returns all active servers which specify C<$cluster> in their readfor key.
    The way in which reads are load-balaned is application dependent. In general,
    it's safe to do reads from any number of slaves, provided that there isn't replication
    lag.

    See also: L<get_failover($cluster)>.

=cut

sub get_read_hosts {
  my ($self, $cluster) = @_;
  my @read_hosts = ();
  foreach my $srv (keys %{$self->{raw}->{'servers'}}) {
  }
}

=pod

=item get_all_hosts()

    Retrieve all hostnames defined.

=cut

sub get_all_hosts {
  my $self = shift;
  return keys %{$self->{raw}->{'servers'}};
}

=pod

=item get_all_clusters()

    Retrieve all clusters defined.

=cut

sub get_all_clusters {
  my $self = shift;
  return keys %{$self->{raw}->{'clusters'}};
}

sub host_active {
  my ($self, $host) = @_;
  return 1 if(exists $self->{raw}->{'servers'}->{$host} and _truth($self->{raw}->{'servers'}->{$host}->{'active'}));
  return 0;
}

sub cluster_active {
  my ($self, $cluster) = @_;
  return 1 if(exists $self->{raw}->{'clusters'}->{$cluster} and _truth($self->{raw}->{'clusters'}->{$cluster}->{'active'}));
  return 0;
}

sub AUTOLOAD {
  my ($self, $host) = @_;
  ref($self) or throw Error->new(-text => "Not an object.");
  my $name = $AUTOLOAD;
  $name =~ s/.*://;
  if($name =~/^server_(.+)$/) {
    return $self->{raw}->{'servers'}->{$host}->{$1};
  }
  if($name =~/^cluster_(.+)$/) {
    return $self->{raw}->{'cluster'}->{$host}->{$1};
  }
  return undef;
}

sub _truth {
  my $str = shift;
  return 1 if($str or $str =~ /(?:[yt]|true|yes)/i);
  return 0 if(!$str or $str =~ /(?:[nf]|false|no)/i);
}
1;
