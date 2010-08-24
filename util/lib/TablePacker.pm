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
package TablePacker;
use strict;
use warnings FATAL => 'all';
use Which;
use Carp;
use DSN;
use DBI;
use Storable;

sub new {
  my $class = shift;
  my ($dsn, $datadir, $dbh) = @_;
  croak("dsn must be a reference to a DSN") unless(ref($dsn));
  my $self = {};
  $self->{datadir} = $datadir;
  $self->{dsn} = $dsn;
  if($dbh) {
    $self->{dbh} = $dbh;
  }
  else {
    $self->{own_dbh} = 1;
    $self->{dbh} = $dsn->get_dbh();
  }
  $self->{schema} = $dsn->get('D');
  $self->{table}  = $dsn->get('t');
  return bless $self, $class;
}

sub STORABLE_freeze {
  my ($self, $cloning) = @_;
  return if $cloning;
  return (
    Storable::nfreeze({
        myisamchk => $self->{myisamchk},
        myisampack => $self->{myisampack},
        datadir => $self->{datadir},
        dsn     => $self->{dsn},
        schema  => $self->{schema},
        table   => $self->{table},
        errstr  => $self->{errstr},
        errval  => $self->{errval}
      })
  );
}

sub STORABLE_thaw {
  my ($self, $cloning, $serialized) = @_;
  return if $cloning;
  my $frst = Storable::thaw($serialized);
  $self->{datadir} = $frst->{datadir};
  $self->{dsn} = $frst->{dsn};
  $self->{schema} = $frst->{schema};
  $self->{table} = $frst->{table};
  $self->{myisamchk} = $frst->{myisamchk};
  $self->{myisampack} = $frst->{myisampack};
  $self->{errstr} = $frst->{errstr};
  $self->{errval} = $frst->{errval};
  return $self;
}

sub STORABLE_attach {
  my ($class, $cloning, $serialized) = @_;
  return if $cloning;
  my $frst = Storable::thaw($serialized);
  my $self;
  # We allow new() to try and auto-vivify a DBI connection,
  # but, we only croak if it was not 'access denied'.
  # This is to support being defrosted somewhere else.
  eval {
    $self = $class->new($frst->{dsn}, $frst->{datadir}, undef);
  };
  if($@ and $@ =~ /DBI connect.*failed: Access denied/i) {
    $self = $class->new($frst->{dsn}, $frst->{datadir}, 'FakeDBH');
  }
  elsif($@) {
    croak($@);
  }
  $self->{myisamchk} = $frst->{myisamchk};
  $self->{myisampack} = $frst->{myisampack};
  $self->{errstr} = $frst->{errstr};
  $self->{errval} = $frst->{errval};
  return $self;
}

sub DESTROY {
  my ($self) = @_;
  if($self->{own_dbh}) {
    $self->{dbh}->disconnect();
  }
}

sub _reconnect {
  my ($self) = @_;
  eval {
    die('Default ping') if($self->{dbh}->ping == 0E0);
  };
  if($@ =~ /^Default ping/) {}
  elsif($@) {
    eval {
      $self->{own_dbh} = 1;
      $self->{dbh} = $self->{dsn}->get_dbh();
    };
    return 1;
  }
  return 0E0;
}

sub myisampack_path {
  my ($self, $path) = @_;
  my $old = $self->{myisampack};
  $self->{myisampack} = $path if( defined $path );
  $old;
}

sub myisamchk_path {
  my ($self, $path) = @_;
  my $old = $self->{myisamchk};
  $self->{myisamchk} = $path if( defined $path );
  $old
}

sub mk_myisam {
  my ($self, $note, $no_replicate) = @_;
  $no_replicate = 1 if(not defined $no_replicate);
  if($note) {
    $note = "/* $note */ ";
  }
  else {
    $note = '';
  }
  $self->_reconnect();
  my ($schema, $table) = ($self->{schema}, $self->{table});
  my $eng = $self->engine();
  my $typ = $self->format();
  my ($log_bin) = $self->{dbh}->selectrow_array('SELECT @@sql_log_bin');
  if($eng ne "myisam" and $typ ne 'compressed') {
    $self->{dbh}->do("SET sql_log_bin=0") if($no_replicate);
    $self->{dbh}->do($note ."ALTER TABLE `$schema`.`$table` ENGINE=MyISAM") or croak("Could not make table myisam");
    $self->{dbh}->do("SET sql_log_bin=$log_bin") if($no_replicate);
    return 1;
  }
  return 1;
}

sub check {
  my ($self) = @_;
  my ($datadir, $schema, $table) =
  ($self->{datadir}, $self->{schema}, $self->{table});
  my $myisamchk = ($self->{myisamchk} ||= Which::which('myisamchk'));
  my ($out, $res);

  $out = qx|$myisamchk -rq "${datadir}/${schema}/${table}" 2>&1|;
  $res = ($? >> 8);

  if($res) {
    $self->{errstr} = $out;
    $self->{errval} = $res;
    croak("Error checking table `$schema`.`$table`");
  }

  return 0;
}

sub flush {
  my ($self) = @_;
  my ($schema, $table) = ($self->{schema}, $self->{table});
  $self->_reconnect();
  $self->{dbh}->do("FLUSH TABLES `$schema`.`$table`");
}

sub pack {
  my ($self) = @_;
  my ($datadir, $schema, $table) =
  ($self->{datadir}, $self->{schema}, $self->{table});
  my $myisampack = ($self->{myisampack} ||= Which::which('myisampack'));
  my ($out, $res);

  $out = qx|$myisampack "${datadir}/${schema}/${table}" 2>&1|;
  $res = ($? >> 8);

  if($res) {
    $self->{errstr} = $out;
    $self->{errval} = $res;
    croak("Error packing table `$schema`.`$table`");
  }

  return 0;
}

sub unpack {
  my ($self) = @_;
  my ($datadir, $schema, $table) =
  ($self->{datadir}, $self->{schema}, $self->{table});
  my $myisamchk = ($self->{myisamchk} ||= Which::which('myisamchk'));
  my ($out, $res);

  $out = qx|$myisamchk --unpack "${datadir}/${schema}/${table}" 2>&1|;
  $res = ($? >> 8);

  if($res) {
    $self->{errstr} = $out;
    $self->{errval} = $res;
    croak("Error checking table `$schema`.`$table`");
  }

  return 0;
}

sub engine {
  my ($self) = @_;
  my ($schema, $table) = ($self->{schema}, $self->{table});
  my $eng;
  $self->_reconnect();
  eval {
    $eng = $self->{dbh}->selectrow_hashref("SHOW TABLE STATUS FROM `$schema` LIKE '$table'")->{'Engine'};
  };
  if($@ =~ /undefined value as a HASH/i) { croak("Table `$schema`.`$table` does not exist") }
  elsif($@) { croak($@); }
  return lc($eng);
}

sub format {
  my ($self) = @_;
  my ($schema, $table) = ($self->{schema}, $self->{table});
  my $typ;
  $self->_reconnect();
  eval {
    $typ = $self->{dbh}->selectrow_hashref("SHOW TABLE STATUS FROM `$schema` LIKE '$table'")->{'Row_format'};
  };
  if($@ =~ /undefined value as a HASH/i) { croak("Table `$schema`.`$table` does not exist") }
  elsif($@) { croak($@); }
  return lc($typ);
}

1;
