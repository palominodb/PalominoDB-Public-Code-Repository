# TableDumper.pm
# Copyright (C) 2013 PalominoDB, Inc.
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

package TableDumper;
use DBI;
use Net::SSH::Perl;
use ProcessLog;
eval "use Math::BigInt::GMP";

sub new {
  my $class = shift;
  my ($dbh, $plog, $user, $host, $pw) = @_;
  my $self = {};
  $self->{dbh} = $dbh;
  $self->{plog} = $plog;
  $self->{user} = $user;
  $self->{host} = $host;
  $self->{pass} = $pw;
  $self->{mysqldump} = "/usr/bin/mysqldump";
  $self->{gzip} = "/usr/bin/gzip";
  $self->{mysqlsocket} = "/tmp/mysql.sock";

  bless $self, $class;
  return $self;
}

sub mysqldump_path {
  my ($self, $path) = @_;
  my $old = $self->{mysqldump};
  $self->{mysqldump} = $path if( defined $path );
  $old;
}

sub gzip_path {
  my ($self, $path) = @_;
  my $old = $self->{gzip};
  $self->{gzip} = $path if( defined $path );
  $old;
}

sub mysqlsocket_path {
  my ($self, $path) = @_;
  my $old = $self->{mysqlsocket};
  $self->{mysqlsocket} = $path if( defined $path );
  $old;
}

sub host {
  my ($self, $new) = @_;
  my $old = $self->{host};
  $self->{host} = $new if( defined $new );
  $old;
}

sub user {
  my ($self, $new) = @_;
  my $old = $self->{user};
  $self->{user} = $new if( defined $new );
  $old;
}

sub pass {
  my ($self, $new) = @_;
  my $old = $self->{pass};
  $self->{pass} = $new if( defined $new );
  $old;
}

sub noop {
  my ($self, $noop) = @_;
  my $old = $self->{noop};
  $self->{noop} = $noop if( defined $noop );
  $old;
}

sub dump {
  my ($self, $dest, $schema, $table_s) = @_;
  my $cmd = $self->_make_mysqldump_cmd($dest, $schema, $table_s);
  $self->{plog}->d("Starting $cmd");
  unless($self->{noop}) {
    eval {
      local $SIG{INT} = sub { die("Command interrupted by SIGINT"); };
      local $SIG{TERM} = sub { die("Command interrupted by SIGTERM"); };
      my $ret = qx/($cmd) 2>&1/;
      if($? != 0) {
        $self->{plog}->e("mysqldump failed with: ". ($? >> 8));
        $self->{plog}->e("messages: $ret");
        die("Error doing mysqldump");
      }
    };
    if($@) {
      chomp($@);
      $self->{plog}->es("Issues with command execution:", $@);
      die("Error doing mysqldump");
    }
    $self->{plog}->d("Completed mysqldump.");
  }
  return 1;
}

sub compress {
  my ($self, $file) = @_;
  unless($self->{dest} or not defined($file)) { # Refuse to compress until after it's been "finished".
    return 0 if(-f "$file.gz"); # gzip appears to refuse compressing if the target exists, and I think that's probably good.
    $self->{plog}->d("Compressing '$file' with $self->{gzip}");
    my $ret = undef;
    unless($self->{noop}) {
      eval {
        local $SIG{INT} = sub { die("Caught SIGINT during compression."); };
        local $SIG{TERM} = sub { die("Caught SIGTERM during compression."); };
        $ret = qx/$self->{gzip} $file 2>&1/;
        if($? != 0) {
          $self->{plog}->e("$self->{gzip} returned: ". ($? >> 8) ."\n", $ret);
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
    $self->{plog}->d("Remote compressing '$file' with $self->{gzip}");
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
        my ( $stdout, $stderr, $exit ) = $self->{ssh}->cmd("$self->{gzip} $file");
        if($exit != 0) {
          $self->{plog}->e("$self->{gzip} returned: ". $exit ."\n", $ret);
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


sub ssh_options {
  my ($self, $opts) = @_;
  my $old = $self->{ssh_options};
  $self->{ssh_options} = $opts if( defined $opts );
  $old;
}

sub remote_dump {
  my ($self, $user, $host, $id, $pass, $dest, $schema, $table_s) = @_;
  my $cmd = $self->_make_mysqldump_cmd($dest, $schema, $table_s);
  eval {
    $self->{ssh} = Net::SSH::Perl->new($host, identity_files => $id, debug => ProcessLog::_PdbDEBUG >= ProcessLog::Level2, options => [$self->{ssh_options}]);
    $self->{plog}->d("Logging into $user\@$host.");
    $self->{ssh}->login($user, $pass);
  };
  if($@) {
    $self->{plog}->e("Unable to login. $@");
    return undef;
  }
  $self->{plog}->d("Running remote mysqldump: '$cmd'");
  unless($self->{noop}) {
    eval {
      local $SIG{INT} = sub { die("Remote command interrupted by SIGINT"); };
      local $SIG{TERM} = sub { die("Remote command interrupted by SIGTERM"); };
      my( $stdout, $stderr, $exit ) = $self->{ssh}->cmd("$cmd");
      if($exit != 0) {
        $self->{plog}->e("Non-zero exit ($exit) from: $cmd");
        $self->{plog}->e("Stderr: $stderr");
        die("Remote mysqldump failed");
      }
    };
    if ($@) {
      chomp($@);
      $self->{plog}->es("Issues with remote command execution:", $@);
      die("Failed to ssh");
    }
    $self->{plog}->d("Completed mysqldump.");
  }
  return 1;
}

sub drop {
  my ($self, $schema, $table_s) = @_;
  my $drops = '';
  if(ref($table_s) eq 'ARRAY') {
    map { $drops .= "`$schema`.`$_`," } @$table_s;
    chop($drops);
  }
  else {
    $drops = "`$schema`.`$table_s`";
  }
  $self->{plog}->d("SQL: DROP TABLE $drops");
  unless($self->{noop}) {
    eval {
      local $SIG{INT} = sub { die("Query interrupted by SIGINT"); };
      local $SIG{TERM} = sub { die("Query interrupted by SIGTERM"); };
      $self->{dbh}->do("DROP TABLE $drops")
        or $self->{plog}->e("Failed to drop some tables.") and die("Failed to drop some tables");
    };
    if($@) {
      chomp($@);
      $self->{plog}->es("Failed to drop some tables:", $@);
      die("Failed to drop some tables");
    }
    $self->{plog}->d("Completed drop.");
  }
  return 1;
}

sub dump_and_drop {
  my ($self, $dest, $schema, $table_s) = @_;
  $self->{plog}->d("Dumping and dropping: ". join(" $schema.", $table_s));
  $self->dump($dest, $schema, $table_s);
  $self->drop($schema, [$table_s]);
  return 1;
}

sub remote_dump_and_drop {
  my ($self, $user, $host, $id, $pass, $dest, $schema, $table_s) = @_;
  $self->remote_dump($user, $host, $id, $pass, $dest, $schema, $table_s);
  $self->drop($schema, [$table_s]);
  return 1;
}

sub _make_mysqldump_cmd {
  my ($self, $dest, $schema, $table_s) = @_;
  my $cmd = qq|if [[ ! -f "$dest.gz" ]]; then $self->{mysqldump} --host $self->{host} --user $self->{user}|;
  $cmd .=" --socket '$self->{mysqlsocket}'" if($self->{host} eq "localhost");
  $cmd .=" --pass='$self->{pass}'" if ($self->{pass});
  $cmd .=" --single-transaction -Q $schema ";
  $cmd .= join(" ", $table_s) if( defined $table_s );
  $cmd .= qq| > "$dest"|;
  $cmd .= qq| ; else echo 'Dump already present.' 1>&2; exit 1 ; fi|;
  $cmd;
}

1;
