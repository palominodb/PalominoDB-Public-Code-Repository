package TablePacker;
use DBI;
use ProcessLog;
use Net::SSH::Perl;
use Sys::Hostname;

sub new {
  my $class = shift;
  my ($dbh, $plog, $host, $user, $id) = @_;
  $self = {};
  $self->{dbh} = $dbh;
  $self->{plog} = $plog;
  $self->{host} = $host;
  $self->{user} = $user;
  $self->{myisampack} = "/usr/bin/myisampack";
  $self->{myisamchk} = "/usr/bin/myisamchk";
  $self->{id} = $id;
  bless $self, $class;
  return $self;
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
  my ($self, $schema, $table) = @_;
  $self->{plog}->d("Converting `$schema`.`$table` to MyISAM, if not already.");
  my $eng = $self->_getengine($schema, $table);
  if(lc($eng) ne "myisam") {
    $self->{plog}->d("`$schema`.`$table` is not myisam -- converting.");
    $self->{dbh}->do("/*". $self->{plog}->name ." on ". hostname() . " */ ALTER TABLE `$schema`.`$table` ENGINE=MyISAM") or $self->{plog}->e("`$schema`.`$table` failed to convert.", DBI->errstr) and die("Error converting table.")
    $self->{plog}->d("`$schema`.`$table` converted to myisam.");
    return 1;
  }
  else {
    $self->{plog}->d("`$schema`.`$table` is already myisam -- not converting.");
    return 1;
  }
  return 1;
}

sub pack {
  my ($self, $datadir, $schema, $table) = @_;
  if($self->_getengine($schema, $table) ne "myisam") {
    $self->{plog}->e("Cannot pack non-myisam table. Found '". $self->_getengine($schema, $table) ."' table.");
    return undef;
  }
  $self->{ssh} = Net::SSH::Perl->new($host, { 'identity_files' => $id, 'debug' => ProcessLog::_PdbDEBUG >= ProcessLog::Level2 });
  eval {
    $self->{plog}->d("Logging into $self->{user}\@$self->{host}");
    $self->{ssh}->login($user);
  };

  if($@) {
    $self->{plog}->e("Unable to login. $@");
    return undef;
  }

  $self->{plog}->d("starting pack of: `$schema`.`$table`");
  $self->{plog}->d("  cmd: $self->{myisampack} ${datadir}/${schema}/${table}");
  eval {
    local $SIG{INT} = sub { die("Remote command interrupted by SIGINT"); };
    local $SIG{TERM} = sub { die("Remote command interrupted by SIGTERM"); };
    my ($stdout, $stderr, $exit) = $ssh->cmd("$self->{myisampack} ${datadir}/${schema}/${table}");
    if($exit != 0) {
      $self->{plog}->e("Non-zero exit status from '$self->{myisampack}'.");
      $self->{plog}->e("remote stderr: $stderr");
      die("Packing failed");
    }
  };
  if($@) {
    chomp($@);
    $self->{plog}->es("Issues with remote command execution:", $@);
    die("Failed to pack");
  }
  $self->{plog}->d("completed pack of: `$schema`.`$table` exit: $exit");

  $self->{plog}->d("starting check of: `$schema`.`$table`");
  $self->{plog}->d("  cmd: $self->{myisamchk} -rq ${datadir}/${schema}/${table}");
  eval {
    local $SIG{INT} = sub { die("Remote command interrupted by SIGINT"); };
    local $SIG{TERM} = sub { die("Remote command interrupted by SIGTERM"); };
    my ($stdout, $stderr, $exit) = $ssh->cmd("$self->{myisamchk} ${datadir}/${schema}/${table}");
    if($exit != 0) {
      $self->{plog}->e("Non-zero exit status from '$self->{myisamchk}'.");
      $self->{plog}->e("remote stderr: $stderr");
      die("Check failed");
    }
  };
  if($@) {
    chomp($@);
    $self->{plog}->es("Issues with remote command execution:", $@);
    die("Failed to check");
  }
  $self->{plog}->d("completed check of: `$schema`.`$table` exit: $exit");
  return 1;
}

sub _getengine {
  my ($self, $schema, $table) = @_;
  $self->{plog}->d("Determining `$schema`.`$table` engine.");
  my $eng = $self->{dbh}->selectrow_hashref("SHOW TABLE STATUS FROM `$schema` LIKE '$table'")->{'Engine'};
  lc($eng);
}
1;
