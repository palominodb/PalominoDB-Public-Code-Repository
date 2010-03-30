package TablePacker;
use Sys::Hostname;
use English qw(-no_match_vars);
use Which;
use Carp;
use DSN;
use DBI;

sub new {
  my $class = shift;
  my ($dsn, $datadir, $dbh) = @_;
  croak("dsn must be a reference to a DSN") unless(ref($dsn));
  $self = {};
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

sub DESTROY {
  my ($self) = @_;
  if($self->{owndbh}) {
    $self->{dbh}->disconnect();
  }
}

sub _reconnect {
  my ($self) = @_;
  eval {
    die('Default ping') if($self->{dbh}->ping == 0E0);
  };
  if($EVAL_ERROR =~ /^Default ping/) {}
  elsif($EVAL_ERROR) {
    eval {
      $self->{dbh}->get_dbh();
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
  my ($self) = @_;
  $self->_reconnect();
  my ($schema, $table) = ($self->{schema}, $self->{table});
  my $eng = $self->engine();
  my $typ = $self->format();
  if($eng ne "myisam" and $typ ne 'compressed') {
    $self->{dbh}->do("/*". $0 ." on ". hostname() . " */ ALTER TABLE `$schema`.`$table` ENGINE=MyISAM") or croak("Could not make table myisam");
    return 1;
  }
  return 1;
}

sub check {
  my ($self, $datadir) = @_;
  my ($datadir, $schema, $table) =
  ($self->{datadir}, $self->{schema}, $self->{table});
  my $myisamchk = ($self->{myisamchk} ||= Which::which('myisamchk'));
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
  my $eng = $self->{dbh}->selectrow_hashref("SHOW TABLE STATUS FROM `$schema` LIKE '$table'")->{'Engine'};
  return lc($eng);
}

sub format {
  my ($self) = @_;
  my ($schema, $table) = ($self->{schema}, $self->{table});
  my $typ = $self->{dbh}->selectrow_hashref("SHOW TABLE STATUS FROM `$schema` LIKE '$table'")->{'Row_format'};
  return lc($typ);
}

1;
