package TableAge;
use strict;
use warnings;
use Data::Dumper;
use DateTime::Format::Strptime;

sub new {
  my $class = shift;
  my ($dbh, $pattern) = @_;
  my $self = {};
  $self->{dbh} = $dbh;
  $self->{pattern} = $pattern;
  $self->{status_dft} = DateTime::Format::Strptime->new(
    pattern => '%F %T', time_zone => "local");
  $self->{name_dft} =  DateTime::Format::Strptime->new(
    pattern => $pattern, time_zone => "local");
  return bless $self, $class;
}

sub age_by_status {
  my ($self, $schema, $table) = @_;
  my $status = $self->{dbh}->selectrow_hashref(qq|SHOW TABLE STATUS FROM `$schema` LIKE '$table'|);
  return $self->{status_dft}->parse_datetime($status->{'Create_time'});
}

sub age_by_name {
  my ($self, $table, $pattern) = @_;
  if($pattern) {
    $self->{name_dft}->pattern($pattern);
  }
  return $self->{name_dft}->parse_datetime($table);
}

1;
