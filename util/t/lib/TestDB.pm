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

my $cnf = {IniFile::read_config($ENV{PDB_SANDBOX_CNF})};
my $port = $cnf->{'mysqld'}->{'port'};
my $socket = $cnf->{'mysqld'}->{'socket'};
our $dsnstr = "h=localhost,u=msandbox,p=msandbox,P=$port,S=$socket";

sub new {
  my ($class, $args) = @_;
  $args ||= {};
  bless $args, $class;

  $args->{dsn} = DSNParser->default()->parse($dsnstr);
  $args->{dbh}  = $args->{dsn}->get_dbh();

  return $args;
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
