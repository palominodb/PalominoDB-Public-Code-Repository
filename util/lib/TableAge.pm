package TableAge;
use strict;
use warnings;
use Data::Dumper;
use DateTime::Format::Strptime;
use ProcessLog;

sub new {
  my $class = shift;
  my ($dbh, $plog, $pattern) = @_;
  my $self = {};
  $self->{dbh} = $dbh;
  $self->{plog} = $plog;
  $self->{pattern} = $pattern;
  bless $self, $class;
  $self->{plog}->d("new TableAge($pattern)");
  return $self;
}

sub age_by_status {
  my ($self, $schema, $table) = @_;
  my $status = $self->{dbh}->selectrow_hashref(qq|SHOW TABLE STATUS FROM `$schema` LIKE '$table'|);
  $self->{plog}->d("Retrieved create_time from show table status");
  $status->{'Create_time'};
}

sub age_by_name {
  my ($self, $table, $pattern) = @_;
  $pattern = $self->{pattern} unless $pattern;
  my $tf = DateTime::Format::Strptime->new(pattern => $pattern, time_zone => "local");
  $self->{plog}->d("Constructed datetime parser.");
  $tf->parse_datetime($table);
}

1;
