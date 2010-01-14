package TestDB;
BEGIN {
  die("Please set PDB_SANDBOX_PORT to the port.") unless defined $ENV{PDB_SANDBOX_PORT};
}
use strict;
use warnings FATAL => 'all';
use DBI;

my $port = $ENV{PDB_SANDBOX_PORT};

sub new {
  my ($class, $args) = @_;
  $args ||= {};
  bless $args, $class;

  $args->{dbh}  = DBI->connect("DBI:mysql:host=localhost;port=$port;mysql_socket=/tmp/mysql_sandbox${port}.sock", 'msandbox', 'msandbox', { PrintError => 0, RaiseError => 1 });

  return $args;
}

sub user {
  return 'msandbox';
}

sub password {
  return 'msandbox';
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
