package RowDumper;
use strict;
use warnings;
use DBI;
use Net::SSH::Perl;
use Data::Dumper;
use ProcessLog;

sub new {
  my $class = shift;
  my ($dbh, $plog, $host, $user, $pass, $schema, $table, $archive_column) = @_;
  my $self = {};
  $self->{dbh} = $dbh;
  $self->{plog} = $plog;
  $self->{host} = $host;
  $self->{user} = $user;
  $self->{pass} = $pass;
  $self->{schema} = $schema;
  $self->{table} = $table;
  $self->{archive_column} = $archive_column;
  $self->{mk_archiver_path} = "/usr/bin/mk-archiver";
  $self->{gzip_path} = "/usr/bin/gzip";
  $self->{dest} = 0;
  $self->{noop} = 0;

  bless $self, $class;

  # Require that the archive_by column be indexed.
  # Otherwise we could potentially fuck ourselves with a full table scan.
  $plog->d("Collecting indexes from `$schema`.`$table`.");
  my $idxs;
  eval {
    $idxs = $dbh->selectrow_hashref("SHOW INDEXES FROM `$schema`.`$table` WHERE column_name=?", {}, $archive_column);
    die("Unable to find indexes") unless(defined $idxs);
    $plog->d("Index name on `$schema`.`$table`:", $idxs->{'Key_name'});
    $self->{archive_index}=$idxs->{'Key_name'};
  };
  if($@) {
    chomp($@);
    $plog->e($@);
    return undef;
  }
  $plog->d("Caching columns from `$schema`.`$table`.");
  @{$self->{columns}} = map {
    $_->[0];
  } @{$dbh->selectall_arrayref("SHOW COLUMNS FROM `$schema`.`$table`")};
  $plog->d("Columns: ", join(",",@{$self->{columns}}));

  return $self;
}

sub noop {
  my ($self, $new) = @_;
  my $old = $self->{noop};
  $self->{noop} = $new if( defined($new) );
  $old;
}

sub reset {
  my $self = shift;
  if($self->{dest}) {
    $self->{plog}->d("Reset dump filehandle.");
    close($self->{dest});
    $self->{dest} = 0;
  }
  1;
}

sub finish {
  my $self = shift;
  if($self->{dest}) {
    $self->{plog}->d("Closed dump filehandle.");
    my $f = $self->{dest};
    print $f "COMMIT;\n";
    close($f);
    $self->{dest} = 0;
  }
  1;
}

sub gzip_path {
  my ($self, $new) = @_;
  my $old = $self->{gzip_path};
  $self->{gzip_path} = $new if( defined($new) );
  $old;
}

sub mk_archiver_path {
  my ($self, $new) = @_;
  my $old = $self->{mk_archiver_path};
  $self->{mk_archiver_path} = $new if( defined($new) );
  $old;
}

sub mk_archiver_opt {
  my ($self, $opt, $new) = @_;
  my $old = $self->{"mk_$opt"};
  $self->{"mko_$opt"} = $new if( defined($new) );
  $old;
}

sub compress {
  my ($self, $file) = @_;
  unless($self->{dest} or not defined($file)) { # Refuse to compress until after it's been "finished".
    return 0 if(-f "$file.gz"); # gzip appears to refuse compressing if the target exists, and I think that's probably good.
    $self->{plog}->d("Compressing '$file' with $self->{gzip_path}");
    my $ret = undef;
    unless($self->{noop}) {
      eval {
        local $SIG{INT} = sub { die("Caught SIGINT during compression."); };
        local $SIG{TERM} = sub { die("Caught SIGTERM during compression."); };
        $ret = qx/$self->{gzip_path} $file 2>&1/;
        if($? != 0) {
          $self->{plog}->e("$self->{gzip_path} returned: ". ($? >> 8) ."\n", $ret);
          die("Failed to compress '$file'");
        }
      };
      if($@) {
        chomp($@);
        $self->{plog}->es($@);
        die("Failed to compress '$file'");
      }
    }
    $self->{plog}->d("Finished compressing '$file'.");
    return 1;
  }
  $self->{plog}->d("Refusing to compress open file: '$file'.");
  return 0;
}

sub remote_compress {
  my ($self, $host, $user, $id, $pass, $file) = @_;
  unless($self->{dest} or not defined($file)) { # Refuse to compress until after it's been "finished".
    #return 0 if(-f "$file.gz"); # gzip appears to refuse compressing if the target exists, and I think that's probably good.
    $self->{plog}->d("Remote compressing '$file' with $self->{gzip_path}");
    eval {
      $self->{ssh} = Net::SSH::Perl->new($host, identity_files => $id, debug => ProcessLog::_PdbDEBUG >= ProcessLog::Level2, options => [$self->{ssh_options}]);
      $self->{plog}->d("Logging into $user\@$host.");
      $self->{ssh}->login($user, $pass);
    };
    if($@) {
      $self->{plog}->e("Unable to login. $@");
      return undef;
    }
    my $ret = undef;
    unless($self->{noop}) {
      eval {
        local $SIG{INT} = sub { die("Caught SIGINT during compression."); };
        local $SIG{TERM} = sub { die("Caught SIGTERM during compression."); };
        my ( $stdout, $stderr, $exit ) = $self->{ssh}->cmd("$self->{gzip_path} $file");
        if($exit != 0) {
          $self->{plog}->e("$self->{gzip_path} returned: ". $exit ."\n", $ret);
          $self->{plog}->e("Stderr: $stderr");
          die("Failed to compress '$file'");
        }
      };
      if($@) {
        chomp($@);
        $self->{plog}->es($@);
        die("Failed to compress '$file'");
      }
    }
    $self->{plog}->d("Finished compressing '$file'.");
    return 1;
  }
  $self->{plog}->d("Refusing to compress open file: '$file'.");
  return 0;
}

sub archive {
  my ($self, $dest, $condition, $limit) = @_;
  my $cmd = $self->_mk_archiver_cmd($condition, $dest);
  $self->{plog}->d("Starting mk-archiver: $cmd");
  eval {
    local $SIG{INT} = sub { die("Caught SIGINT during mk-archiver."); };
    local $SIG{TERM} = sub { die("Caught SIGTERM during mk-archiver."); };
    # We could very well just run something like mk_archiver::main(shellwords($cmd)),
    # But that would prevent capturing stdout, and stderr, something which is useful to have,
    # since we can't overload print().
    my $out = qx($cmd 2>&1); 
    $self->{plog}->m($out) if($self->{'noop'});
    if($? != 0) {
      $self->{plog}->e("mk-archiver failed with: ". ($? >> 8));
      $self->{plog}->e("messages: $out");
      die("Error doing mk-archiver");
    }
  };
  if($@) {
    chomp($@);
    $self->{plog}->es("Issues with command execution:", $@);
    die("Error doing mk-archiver");
  }
  $self->{plog}->d("Finished mk-archiver.");
  1;
}

sub remote_archive {
  my ($self, $host, $user, $id, $pass, $dest, $condition, $limit) = @_;
  my $cmd = $self->_mk_archiver_cmd($condition, $dest);
  eval {
    $self->{ssh} = Net::SSH::Perl->new($host, identity_files => $id, debug => ProcessLog::_PdbDEBUG >= ProcessLog::Level2, options => [$self->{ssh_options}]);
    $self->{plog}->d("Logging into $user\@$host.");
    $self->{ssh}->login($user, $pass);
  };
  if($@) {
    $self->{plog}->e("Unable to login: $@");
    return undef;
  }
  $self->{plog}->d("Running remote mk-archiver: '$cmd'");
  eval {
    local $SIG{INT} = sub { die("Remote command interrupted by SIGINT"); };
    local $SIG{TERM} = sub { die("Remote command interrupted by SIGTERM"); };
    my( $stdout, $stderr, $exit ) = $self->{ssh}->cmd("$cmd");
    if($exit != 0) {
      $self->{plog}->e("Non-zero exit ($exit) from: $cmd");
      $self->{plog}->e("Stderr: $stderr");
      die("Remote mk-archiver failed");
    }
  };
  if ($@) {
    chomp($@);
    $self->{plog}->es("Issues with remote command execution:", $@);
    die("Failed to ssh");
  }
  $self->{plog}->d("Finished remote mk-archiver.");
  return 1;
}

sub simple_dump {
  my ($self, $dest, $condition, $limit, $bindvars) = @_;

  my $comment = $self->{plog}->name() . " - RowDumper";
  my $limstr = defined($limit) ? "LIMIT ?" : "";
  $self->{plog}->d("Dumping: $self->{schema}.$self->{table} $condition". (defined($limit) ? ", $limit" : "") .", ". join(",", $bindvars));
  $self->{plog}->d("Dump SQL: /* $comment */ SELECT * FROM `$self->{schema}`.`$self->{table}` WHERE ($condition) $limstr");
  my $sth = $self->{dbh}->prepare_cached(qq#/* $comment */ SELECT * FROM `$self->{schema}`.`$self->{table}` WHERE ($condition) $limstr#);

  my $ret = (defined($limit) ? $sth->execute($bindvars,$limit) : $sth->execute($bindvars));
  $self->{plog}->d("Dump execute returns: $ret");
  my $i = 0;
  unless($self->{noop}) {
    while ( my $r = $sth->fetch ) {
      $self->_writerow($dest, $r);
      $i++;
    }
  }
  $self->{plog}->d("No rows dumped.") if($i == 0);
  $i;
}

sub simple_drop {
  my ($self, $condition, $limit, $bindvars) = @_;
  my $comment = $self->{plog}->name() . " - RowDumper";
  my $limstr = defined($limit) ? "LIMIT ?" : "";
  $self->{plog}->d("Deleting: $self->{schema}.$self->{table} $condition, $limit, ". join(",", $bindvars));
  $self->{plog}->d("Delete SQL: /* $comment */ DELETE FROM `$self->{schema}`.`$self->{table}` WHERE ($condition) $limstr");
  my $sth = $self->{dbh}->prepare_cached(qq#/* $comment */ DELETE FROM `$self->{schema}`.`$self->{table}` WHERE ($condition) $limstr#);

  my $ret = 0;
  unless($self->{noop}) {
    $ret = (defined($limit) ? $sth->execute($bindvars,$limit) : $sth->execute($bindvars));
  }
  $self->{plog}->d("No rows dropped.") if($ret == 0 or $ret == 0E0);
  $ret;
}

sub dumpgt {
  my ($self, $dest, $condvar, $rowlim) = @_;
  $self->dump($dest, "$self->{archive_column}>=?", $rowlim, $condvar);
}

sub dumplt {
  my ($self, $dest, $condvar, $rowlim) = @_;
  $self->dump($dest, "$self->{archive_column}<=?", $rowlim, $condvar);
}

sub dropgt {
  my ($self, $condvar, $rowlim) = @_;
  $self->drop("$self->{archive_column}>=?", $rowlim, $condvar);
}

sub droplt {
  my ($self, $condvar, $rowlim) = @_;
  $self->drop("$self->{archive_column}<=?", $rowlim, $condvar);
}

sub _writerow {
  my ($self, $dest, $r) = @_;
  my $f;
  unless($self->{dest}) {
    $self->{plog}->d("Opened dumpfile: $dest");
    open $f, ">>$dest";
    print $f "USE `$self->{schema}`;\n";
    print $f "BEGIN;\n";
    $self->{dest} = $f;
  }
  else {
    $f = $self->{dest};
  }

  ProcessLog::_PdbDEBUG >= ProcessLog::Level3 && $self->{plog}->d("writerow: rowdata: ". join(",", @$r));

  # Converts an arrayref row into a db-quoted string for insertion.
  # Quotes even pure integer values, and, testing indicates that's fine.
  my $insdata = join(",", map {
      $self->{dbh}->quote($_);
    } @$r);
  print $f "INSERT INTO `$self->{table}` (". join(",",@{$self->{columns}}) .") VALUES ($insdata);\n";
}

sub _mk_archiver_cmd {
  my ($self, $condition, $dest) = @_;
  unless($self->{mk_archiver_path}) {
    $self->{plog}->es(qq#Invalid path to mk-archiver: "$self->{mk_archiver_path}"#);
    die("Path to mk-archiver invalid");
  }
  my $cmd = "perl $self->{mk_archiver_path} --source h=$self->{host},u=$self->{user},p=$self->{pass},D=$self->{schema},t=$self->{table},i=$self->{archive_index} ";
  $cmd .= "--where \"$condition\" ";
  $cmd .= "--file \"$dest\" " unless($self->{'mko_dest'});
  $cmd .= join(" ", map {
      if (/^mko_(.*)$/) {
        $_ = qq|--$1 $self->{"mko_$1"}|;
      }
    } keys %$self);
  $cmd .= " --dry-run" if($self->{'noop'});
  $cmd;
}

1;
