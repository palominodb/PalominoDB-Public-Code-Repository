# YAMLDSN.pm - A Perl interface to the PalominoDB DSN format.
# Copyright (C) 2009-2013 PalominoDB, Inc.
# 
# You may contact the maintainers at eng@palominodb.com.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

package YAMLDSN;
=pod

=head1 NAME

YAMLDSN - Perl interface to the PalominoDB DSN format.

=head1 SYNOPSIS

    my $dsn = YAMLDSN->new("http://int.example.com/dsn/dsn.yml");
    $dsn    = YAMLDSN->new("/path/to/dsn.yml");

    $dsn->get_all_hosts();         # => ['s1', 's2']
    $dsn->get_all_clusters();      # => ['c1']

    $dsn->cluster_primary('c1');   # => 's1'
    $dsn->cluster_failover('c2');  # => 's2'

    $dsn->host_active('s1');       # => 1 (active)
    $dsn->host_active('s2');       # => 0 (not active)

=head1 DYNAMIC METHODS

PalominoDB DSN format allows for arbitrary application-specific keys
to be included in the file to support operational processes.
Since, extending the dsn for every imaginable key makes little sense
YAMLDSN exposes these via 'dynamic methods'.

Presently, there are two kinds of dynamic methods:

=over 8

=item server methods

These methods start with C<server_> followed by the application specific
key name. They always take a server name as an argument.
Example:

    $dsn->server_delay('s1');

This would retrieve: servers->s1->delay from the dsn.

=item cluster methods

These are the same as server methods, except that they start with
C<cluster_> and take a cluster name as an argument.

=back

=head1 METHODS

=cut

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

=head3 new($uri)

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

=head3 open($uri)

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

=head3 from_hash($hashref)

    hashref: Reference to a hash conforming to the DSN specification.

    It's strongly recommended to run validate after calling this.

=cut

sub from_hash {
  my ($self, $args) = @_;
  $self->{raw} = $args;
  return 1;
}

=pod

=head3 validate()

    This method, B<when implemented>, will validate that the DSN conforms to all
    requirements.

=cut

sub validate {
  my $self = shift;
  return 1;
}

=pod

=head3 get_write_hosts($cluster)

    Retrieve destinations for writes for C<$cluster>.

    This method returns all active servers which specify C<$cluster> in their writefor key.
    The way in which writes are load-balaned is application dependent. In general,
    only one database should take writes at any given time.

    See also: L<cluster_primary($cluster)>.

=cut

sub get_write_hosts {
  my ($self, $cluster) = @_;
  my @write_hosts = ();
  foreach my $srv (@{$self->{raw}->{'clusters'}->{$cluster}->{'servers'}}) {
    if(exists $self->{raw}->{'servers'}->{$srv}) {
      my $writefor = $self->{raw}->{'servers'}->{$srv}->{'writefor'};
      if(ref($writefor) eq 'ARRAY' and grep /$cluster/, @$writefor) {
        push @write_hosts, $srv;
      }
      elsif(!ref($writefor) and $writefor eq $cluster) {
        push @write_hosts, $srv;
      }
    }
  }
  return @write_hosts;
}

=pod

=head3 get_read_hosts($cluster)

    Retrieve destinations for reads for C<$cluster>.

    This method returns all active servers which specify C<$cluster> in their readfor key.
    The way in which reads are load-balaned is application dependent. In general,
    it's safe to do reads from any number of slaves, provided that there isn't replication
    lag.

    See also: L<cluster_failover($cluster)>.

=cut

sub get_read_hosts {
  my ($self, $cluster) = @_;
  my @read_hosts = ();
  foreach my $srv (@{$self->{raw}->{'clusters'}->{$cluster}->{'servers'}}) {
    if(exists $self->{raw}->{'servers'}->{$srv}) {
      my $readfor = $self->{raw}->{'servers'}->{$srv}->{'readfor'};
      if(ref($readfor) eq 'ARRAY' and grep /$cluster/, @$readfor) {
        push @read_hosts, $srv;
      }
      elsif(!ref($readfor) and $readfor eq $cluster) {
        push @read_hosts, $srv;
      }
    }
  }
  return @read_hosts;
}


=pod

=head3 get_all_hosts()

    Retrieve all hostnames defined.

=cut

sub get_all_hosts {
  my $self = shift;
  return keys %{$self->{raw}->{'servers'}};
}

=pod

=head3 get_all_clusters()

    Retrieve all clusters defined.

=cut

sub get_all_clusters {
  my $self = shift;
  return keys %{$self->{raw}->{'clusters'}};
}

=pod

=head3 host_active()

Returns 1 if the given host is active, returns 0 otherwise.

=cut

sub host_active {
  my ($self, $host) = @_;
  return 1 if(exists $self->{raw}->{'servers'}->{$host} and _truth($self->{raw}->{'servers'}->{$host}->{'active'}));
  return 0;
}

=pod

=head3 cluster_active()

Returns 1 if the given cluster is active, returns 0 otherwise.

=cut

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
  elsif($name =~/^cluster_(.+)$/) {
    return $self->{raw}->{'clusters'}->{$host}->{$1};
  }
  elsif($name =~ /^config_(.+)$/) {
    return $self->{raw}->{'config'}->{$1};
  }
  return undef;
}

sub _truth {
  my $str = shift;
  return 1 if($str or $str =~ /(?:[yt]|true|yes)/i);
  return 0 if(!$str or $str =~ /(?:[nf]|false|no)/i);
}
1;

