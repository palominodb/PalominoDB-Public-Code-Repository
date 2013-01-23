# TestDB.pm - rubicund
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
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301 USA.

package TestDB;
BEGIN {
  die("Please set PDB_SANDBOX_CNF to the my.cnf") unless($ENV{'PDB_SANDBOX_CNF'});
  die("Please ensure PDB_SANDBOX_CNF is readable/exists") unless( -f $ENV{'PDB_SANDBOX_CNF'} )
}
use strict;
use warnings FATAL => 'all';
use DBI;
use DSN;
use IniFile;
use File::Basename;

our $cnf = {IniFile::read_config($ENV{PDB_SANDBOX_CNF})};
our $port = $cnf->{'mysqld'}->{'port'};
our $socket = $cnf->{'mysqld'}->{'socket'};
our $dsnstr = "h=localhost,u=root,p=msandbox,P=$port,S=$socket";

sub new {
  my ($class, $args) = @_;
  $args ||= {};
  bless $args, $class;

  $args->{dsn} = DSNParser->default()->parse($dsnstr);
  $args->{dbh}  = $args->{dsn}->get_dbh();
  # Create a user with no super privilege
  $args->{dbh}->do(q|
    GRANT SELECT, INSERT, UPDATE, DELETE, CREATE,
    DROP, RELOAD, SHUTDOWN, PROCESS, FILE, INDEX, ALTER,
    SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES,
    EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT,
    CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE
      ON *.* TO 'nosuper'@'%' IDENTIFIED BY 'superpw'
  |);

  return $args;
}

sub stop {
  my ($self) = @_;
  system(dirname($ENV{PDB_SANDBOX_CNF}) ."/stop");
}

sub start {
  my ($self) = @_;
  system(dirname($ENV{PDB_SANDBOX_CNF}) ."/start");
}

sub fsclear {
  my ($self) = @_;
  system(dirname($ENV{PDB_SANDBOX_CNF}) ."/clear");
}

sub DESTROY {
  my ($self) = @_;
  $self->{dbh}->disconnect();
}

sub datadir {
  return $cnf->{'mysqld'}->{'datadir'};
}

sub user {
  my ($self) = @_;
  return $self->{dsn}->get('u');
}

sub password {
  my ($self) = @_;
  return $self->{dsn}->get('p');
}

sub rand_data {
  my ($self, $file, $always_new, @args) = @_;
  my $fh;
  unshift(@args, '-o', $file);
  if(! -f $file || $always_new) {
    system($ENV{'PDB_CODE_ROOT'} . '/tools/gen_tbdata.pl', @args);
  }
  eval {
    $self->{dbh}->{AutoCommit} = 0;
    open($fh, '<', $file);
    while(<$fh>) {
      chomp;
      next unless($_);
      $self->{dbh}->do($_);
    }
    $self->{dbh}->commit;
    $self->{dbh}->{AutoCommit} = 0;
  };
  if($@) {
    $_ = "$@";
    $self->{dbh}->rollback;
    croak($_);
  }
}

sub use {
  my ($self, $name) = @_;
  eval {
    $self->{dbh}->do("USE $name");
  };
  if($@) {
    $self->create_schema($name);
    $self->{dbh}->do("USE $name");
  }
}

sub create_schema {
  my ($self, $name) = @_;
  $self->{dbh}->do("CREATE DATABASE IF NOT EXISTS `$name`");
}

sub dsn {
  my ($self) = @_;
  $self->{dsn}->str();
}

sub dbh {
  my ($self) = @_;
  $self->{dbh};
}

sub clean_db {
  my ($self) = @_;
   foreach my $db ( @{$self->{dbh}->selectcol_arrayref('SHOW DATABASES')} ) {
      next if $db eq 'mysql';
      next if $db eq 'information_schema';
      $self->{dbh}->do("DROP DATABASE IF EXISTS `$db`");
   }
   return;
}
