package RowDumper;
use strict;
use warnings;
use DBI;
use Net::SSH::Perl;
use Data::Dumper;
use ProcessLog;

sub new {
  my $class = shift;
  my ($dbh, $plog, $schema, $table, $archive_column) = @_;
  my $self = {};
  $self->{dbh} = $dbh;
  $self->{plog} = $plog;
  $self->{schema} = $schema;
  $self->{table} = $table;
  $self->{archive_column} = $archive_column;
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
  };
  if($@) {
    chomp($@);
    $plog->e($@, DBI::errstr.".");
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

sub dump {
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

sub drop {
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

1;
