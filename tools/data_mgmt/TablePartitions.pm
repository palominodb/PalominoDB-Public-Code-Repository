package TablePartitions;
use strict;
use warnings FATAL => 'all';

use English qw(-no_match_vars);
use Data::Dumper;

use ProcessLog;

sub new {
  my ( $class, $pl, $dbh, $schema, $name ) = @_;
  my $self = ();
  $self->{dbh} = $dbh;
  $self->{pl} = $pl;
  $self->{schema} = $schema;
  $self->{name} = $name;
  bless $self, $class;

  $self->_get_partitions();

  return $self;
}

sub _get_version {
  my ($self) = @_;
  my $dbh = $self->{dbh};

  my ($version) = $dbh->selectrow_array('SELECT VERSION()');
  my ($major, $minor, $micro, $dist) = $version =~ /^(\d+)\.(\d+)\.(\d+)-(.*)/;
  ["$major.$minor", $major, $minor, $micro, $dist];
}

sub _get_partitions {
  my ($self) = @_;
  my $dbh = $self->{dbh};
  my ($release, undef, undef, undef, undef) = $self->_get_version();
  die("Server release not at least 5.1 ($release)") if ($release < 5.1);

  # Placeholder for future methods of getting information.
  if(1) {
    $self->_get_partitions_by_IS();
  }
}

# Get partition information via information_schema.
sub _get_partitions_by_IS {
  my ($self) = @_;
  my $dbh = $self->{dbh};

  my $qtd_schema = $dbh->quote($self->{schema});
  my $qtd_table  = $dbh->quote($self->{name});

  my $rows = $dbh->selectall_arrayref(
    "SELECT * FROM `information_schema`.`PARTITIONS` WHERE TABLE_SCHEMA=$qtd_schema AND TABLE_NAME=$qtd_table",
    { Slice => {} });

  $self->{partitions} = [];
  $self->{partition_method} = $rows->[0]->{PARTITION_METHOD};
  $self->{partition_expression} = $rows->[0]->{PARTITION_EXPRESSION};
  $self->{pl}->es("Table does not have any partitions") and die("Table does not have any partitions") unless(scalar @$rows >= 1);
  foreach my $r (@$rows) {
    my $p = {
      name => $r->{PARTITION_NAME},
      sub_name => $r->{SUBPARTITION_NAME},
      position => $r->{PARTITION_ORDINAL_POSITION},
      description => $r->{PARTITION_DESCRIPTION},
      sub_position => $r->{SUBPARTITION_ORDINAL_POSITION}
    };
    push @{$self->{partitions}}, $p;
  }
}

# Return all partitions
sub partitions {
  my ($self) = @_;
  $self->{pl}->d(Dumper($self->{partitions}));
  $self->{partitions}
}

sub first_partition {
  my ($self) = @_;
  $self->{partitions}->[0];
}

sub last_partition {
  my ($self) = @_;
  $self->{partitions}->[-1];
}

sub method {
  my ($self) = @_;
  $self->{partition_method};
}

sub expression {
  my ($self) = @_;
  $self->{partition_expression};
}

# Returns true if the partitioning expression
# looks date-like. i.e., has to_days(<column>).
# TODO: Add additional methods?
sub expr_datelike {
  my ($self) = @_;
  $self->{partition_expression} =~ /^to_days\(([A-Za-z0-9\-_\$]+)\)/;
  return $1;
}

# Return partitions (Not sub-partitions) that match $reg
sub match_partitions {
  my ($self, $reg) = @_;
  my %res;
  map { $res{$_->{name}} = {name => $_->{name}, position => $_->{position}, description => $_->{description} } if($_->{name} =~ $reg); } @{$self->{partitions}};
  values %res;
}

# True on success, undef on failure.
sub add_range_partition {
  my ($self, $name, $description, $pretend) = @_;
  if($self->method ne 'RANGE') {
    $self->{pl}->m("Unable to add partition to non-RANGE partition scheme.");
    return undef;
  }
  for my $p (@{$self->{partitions}}) {
    if($p->{description} eq 'MAXVALUE') {
      $self->{pl}->m("Unable to add new partition when a catchall partition ($p->{name}) exists.");
      return undef;
    }
  }
  my $qtd_desc = $self->{dbh}->quote($description);
  $self->{pl}->d("ALTER TABLE `$self->{schema}`.`$self->{name}` ADD PARTITION (PARTITION $name VALUES LESS THAN (to_days($qtd_desc)))");
  eval {
    unless($pretend) {
      $self->{dbh}->do("ALTER TABLE `$self->{schema}`.`$self->{name}` ADD PARTITION (PARTITION $name VALUES LESS THAN (to_days($qtd_desc)))");
      $self->_add_part($name, "to_days($qtd_desc)");
    }
  };
  if($EVAL_ERROR) {
    $self->{pl}->e("Error adding partition: $@");
    return undef;
  }
  return 1;
}

sub drop_partition {
  my ($self, $name, $pretend) = @_;
  if($self->method ne 'RANGE') {
    $self->{pl}->m("Unable to drop partition from non-RANGE partition scheme.");
    return undef;
  }
  $self->{pl}->d("ALTER TABLE `$self->{schema}`.`$self->{name}` DROP PARTITION $name");
  eval {
    unless($pretend) {
      $self->{dbh}->do("ALTER TABLE `$self->{schema}`.`$self->{name}` DROP PARTITION $name");
      $self->_del_part($name);
    }
  };
  if($EVAL_ERROR) {
    $self->{pl}->e("Error dropping partition: $@");
    return undef;
  }

  return 1;
}

# If the partitioning expression is date-like,
# then return the description like a datestamp.
# Else, return undef.
sub desc_from_days {
  my ($self, $name) = @_;
  return undef if(!$self->expr_datelike);
  if($self->method ne 'RANGE') {
    $self->{pl}->d("Only makes sense for RANGE partitioning.");
    return undef;
  }
  my $desc = undef;
  for my $p (@{$self->{partitions}}) {
    if($p->{name} eq $name) {
      $desc = $p->{description};
      last;
    }
  }
  my ($ds) = $self->{dbh}->selectrow_array("SELECT FROM_DAYS($desc)");
  return $ds;
}


sub _add_part {
  my ($self, $name, $desc) = @_;
  my ($d) = $self->{dbh}->selectrow_array("SELECT $desc");
  push @{$self->{partitions}}, {name => $name, description => $d, position => undef};
}

sub _del_part {
  my ($self, $name) = @_;
  my @replace = ();
  foreach my $p (@{$self->{partitions}}) {
    unless($p->{name} eq $name) {
      push @replace, $p;
    }
  }
  $self->{partitions} = \@replace;
}

1;

