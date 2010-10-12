# Copyright (c) 2009-2010, PalominoDB, Inc.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
# 
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
# 
#   * Neither the name of PalominoDB, Inc. nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
package DSN;
use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Storable;

# DSN objects should only be created
# by a DSNParser object.
sub _create {
  my ($class, $keys) = @_;
  my $self = {};
  $self = _merge($self, $keys);
  return bless $self, $class;
}

sub STORABLE_freeze {
  my ($self, $cloning) = @_;
  return if $cloning;
  my $f = {};
  _merge($f, $self);
  return (
    Storable::nfreeze($f)
  );
}

sub STORABLE_thaw {
  my ($self, $cloning, $serialized) = @_;
  return if $cloning;
  my $f = Storable::thaw($serialized);
  return _merge($self, $f);
}

sub STORABLE_attach {
  my ($class, $cloning, $serialized) = @_;
  return if $cloning;
  my $f = Storable::thaw($serialized);
  return $class->_create($f);
}

## This stub prevents AUTOLOAD and others
## from messing with us under some circumstances
sub DESTROY {}

sub get {
  my ($self, $k) = @_;
  return $self->{$k}->{'value'};
}

sub has {
  my ($self, $k) = @_;
  return exists $self->{$k}->{'value'};
}

sub str {
  my ($self) = @_;
  my $str = "";
  for(sort keys %$self) {
    $str .= "$_=". $self->get($_) ."," if($self->has($_));
  }
  chop($str);
  return $str;
}

sub get_dbi_str {
  my ($self) = @_;
  my %set_implied = ();
  my %dsn_conv = (
    'h' => 'host',
    'P' => 'port',
    'F' => 'mysql_read_default_file',
    'G' => 'mysql_read_default_group',
    'S' => 'mysql_socket',
    'D' => 'database',
    'SSL_key' => 'mysql_ssl_client_key',
    'SSL_cert' => 'mysql_ssl_client_cert',
    'SSL_CA' => 'mysql_ssl_ca_file',
    'SSL_CA_path' => 'mysql_ssl_ca_path',
    'SSL_cipher' => 'mysql_ssl_cipher'
  );
  # options that are implied by the presense of another
  # only one instance of an implied option will be added to the
  # dbi string
  my %opt_implied = (
    'SSL_key' => 'mysql_ssl=1',
    'SSL_cert' => 'mysql_ssl=1',
    'SSL_CA' => 'mysql_ssl=1',
    'SSL_CA_path' => 'mysql_ssl=1',
    'SSL_cipher' => 'mysql_ssl=1'
  );
    
  my $dbh_str = 'DBI:mysql:';

  for(sort keys(%$self)) {
    if(exists($opt_implied{$_}) and $self->has($_) and !$set_implied{$opt_implied{$_}}) {
      $dbh_str .= $opt_implied{$_} . ';';
      $set_implied{$opt_implied{$_}} = 1;
    }
    $dbh_str .= $dsn_conv{$_} .'='. ($self->get($_) || '') .';'
    if(exists($dsn_conv{$_}) and $self->has($_));
  }
  return $dbh_str;
}

sub get_dbh {
  my ($self, $cached) = @_;
  my $dbh_str = $self->get_dbi_str();
  my $dbh;

  if($cached) {
    $dbh = DBI->connect_cached($dbh_str, $self->get('u'), $self->get('p'),
      { 'AutoCommit' => 0, 'RaiseError' => 1,
        'PrintError' => 0, 'ShowErrorStatement' => 1 });
  }
  else {
    $dbh = DBI->connect($dbh_str, $self->get('u'), $self->get('p'),
      { 'AutoCommit' => 0, 'RaiseError' => 1,
        'PrintError' => 0, 'ShowErrorStatement' => 1 });
  }
  return $dbh;
}

sub fill_in {
  my ($self, $from) = @_;
  $self = _merge($self, $from, 0);
  return $self;
}

# Dumb in-place merge of %$h2 into %$h1
# Returns $h1
# Also has side-effect of creating a copy of %$h2
# when %$h1 is empty.
sub _merge {
  my ($h1, $h2, $over, $p) = @_;
  foreach my $k (keys %$h2) {
    if(!ref($h2->{$k})) {
      if($over and exists $h1->{$k}) {
        $h1->{$k} = $h2->{$k};
      }
      elsif(!exists $h1->{$k}) {
        $h1->{$k} = $h2->{$k};
      }
    }
    elsif(ref($h2->{$k}) eq 'ARRAY') {
      $h1->{$k} = [];
      push @{$h1->{$k}}, $_ for(@{$h2->{$k}});
    }
    else {
      $h1->{$k} ||= {};
      _merge($h1->{$k}, $h2->{$k}, $over, $h1);
    }
  }
  $h1;
}

1;

=pod

=head1 NAME

DSN - Represents a DSN

=head1 SYNOPSIS

This DSN class is more or less compatible with L<http://maatkit.org/> style DSNs.
The format of DSN strings is the same, and many of the same keys are reused.
This class does have fewer methods than the maatkit DSN package, but, that is
one of the few differences.

The principal difference being that C<parse()> returns a reference to a hash
blessed into the C<DSN> class instead of just a hashref. This difference is
mostly acedemic for consumers of this class, it just enables a slightly different
usage pattern.

=head1 DSN STRINGS

DSN Strings, such as those passed as option values, or arguments to a program
are of the format: C<({key}={value}(,{key}={value})*>. That is, a C<key=value> pair, followed
by a comma, followed by any number of additional C<key=value> pairs separated by
commas.

Examples:

  h=testdb1,u=pdb,p=frogs
  h=localhost,S=/tmp/mysql.sock,u=root,F=/root/my.cnf

=cut

package DSNParser;
use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Carp;

sub new {
  my ($class, $keys) = @_;
  croak('keys must be a hashref') unless(ref($keys));
  my $self = {};
  $self->{'keys'} = $keys;
  $self->{'allow_unknown'} = 0;
  return bless $self, $class;
}

sub add_key {
  my ($self, $key, $params) = @_;
  croak('params must be a hashref') unless(ref($params));
  if(exists $self->{'keys'}->{$key}) {
    croak("Key '$key' must not already exist");
  }
  $self->{'keys'}->{$key} = $params;
}

sub rem_key {
  my ($self, $key) = @_;
  unless(exists $self->{'keys'}->{$key}) {
    croak("Key '$key' must already exist");
  }
  delete $self->{'keys'}->{$key};
}

sub mand_key {
  my ($self, $key, $flag) = @_;
  unless(exists $self->{'keys'}->{$key}) {
    croak("Key '$key' must already exist");
  }
  $self->{'keys'}->{$key}->{'mandatory'} = $flag;
}

sub default {
  my ($class) = @_;
  my $default_keys = {
    'h' => {
      'desc' => 'Hostname',
      'default' => '',
      'mandatory' => 0
    },
    'u' => {
      'desc' => 'Username',
      'default' => '',
      'mandatory' => 0
    },
    'p' => {
      'desc' => 'Password',
      'default' => '',
      'mandatory' => 0
    },
    'P' => {
      'desc' => 'Port',
      'default' => 3306,
      'mandatory' => 0
    },
    'F' => {
      'desc' => 'Defaults File',
      'default' => '',
      'mandatory' => 0
    },
    'G' => {
      'desc' => 'Defaults File Group',
      'default' => 'client',
      'mandatory' => 0
    },
    'D' => {
      'desc' => 'Database name',
      'default' => '',
      'mandatory' => 0
    },
    't' => {
      'desc' => 'Table name',
      'default' => '',
      'mandatory' => 0
    },
    'S' => {
      'desc' => 'Socket path',
      'default' => '',
      'mandatory' => 0
    },
    'sU' => {
      'desc' => 'SSH User',
      'default' => '',
      'mandatory' => 0
    },
    'sK' => {
      'desc' => 'SSH Key',
      'default' => '',
      'mandatory' => 0
    },
    'SSL_key' => {
      'desc' => 'SSL client key',
      'default' => '',
      'mandatory' => 0
    },
    'SSL_cert' => {
      'desc' => 'SSL client certificate',
      'default' => '',
      'mandatory' => 0
    },
    'SSL_CA' => {
      'desc' => 'SSL client CA file',
      'default' => '',
      'mandatory' => 0
    },
    'SSL_CA_path' => {
      'desc' => 'SSL client CA path',
      'default' => '',
      'mandatory' => 0
    },
    'SSL_cipher' => {
      'desc' => 'SSL cipher',
      'default' => '',
      'mandatory' => 0
    }
  };
  return $class->new($default_keys);
}

sub parse {
  my ($self, $str) = @_;
  use Data::Dumper;
  $Data::Dumper::Indent = 0;
  my $dsn = DSN->_create($self->{'keys'});
  foreach my $kv ( split(/,/, $str) ) {
    my ($key, $val) = split(/=/, $kv);
    croak('Unknown key: '. $key .' in dsn')
    unless($self->{'allow_unknown'} or exists($self->{'keys'}->{$key}) );
    $dsn->{$key}->{'value'} = $val;
  }
  foreach my $k ( keys %$dsn ) {
    if($dsn->{$k}->{'mandatory'} and ! exists($dsn->{$k}->{'value'})) {
      croak('Missing key: '. $k .' ['. ($self->{'keys'}->{$k}->{'desc'}||'no description') .'] in dsn');
    }
  }
  return $dsn;
}

=pod

=head1 NAME

DSNParser - Parse DSNs into DSN objects

=head1 SYNOPSIS

=head1 PREDEFINED KEYS

A DSN may have any number of keys, including keys not in this list for
application specific purposes, but, the keys in this list must be used
for their intended purpose. While the DSN class cannot enforce proper usage,
libraries that use this DSN class should conform to this standard.

See: C<default()> for a parser which handles just this set.

=over 8

=item C<h>

Hostname or IP address.

=item C<u>

MySQL Username.

=item C<p>

MySQL Password.

=item C<F>

Defaults file.

=item C<S>

MySQL socket.

=item C<P>

MySQL port.

=item C<D>

Database name.

=item C<t>

Table name.

=item C<sU>

SSH user.

=item C<sK>

SSH key.

=back

=cut

1;
