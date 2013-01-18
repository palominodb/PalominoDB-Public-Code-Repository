# TableRotater.pm
# Copyright (C) 2009-2013 PalominoDB, Inc.
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

package TableRotater;
use DBI;
use DSN;
use DateTime;
use Carp;

sub new {
  my $class = shift;
  my ($dsn, $format, $dbh) = @_;
  $format ||= "%Y%m%d";
  my $self = {};
  if($dbh) {
    $self->{dbh} = $dbh;
  }
  else {
    $self->{dbh} = $dsn->get_dbh();
    $self->{own_dbh} = 1;
  }
  $self->{format} = $format;

  return bless $self, $class;
}

sub DESTROY {
  my ($self) = @_;
  if($self->{own_dbh}) {
    $self->{dbh}->disconnect();
  }
}

sub date_rotate_name {
  my ($self, $table, $dt) = @_;
  $dt ||= DateTime->now(time_zone => 'local');
  my $rot_table = $dt->strftime("${table}$self->{format}");
}

sub rand_str {
  my ($self) = @_;

  my @chars=('a'..'z','A'..'Z','0'..'9','_');
  my $random_string;
  foreach (1..16) {
    # rand @chars will generate a random 
    # number between 0 and scalar @chars
    $random_string.=$chars[rand @chars];
  }
  return $random_string;
}

sub table_for_date {
  my ($self, $schema, $table, $dt) = @_;
  my $rot_table = $self->date_rotate_name($table, $dt);
  $self->{dbh}->selectrow_hashref(
    qq|SHOW TABLE STATUS FROM `$schema` LIKE '$rot_table'|
  );
}

sub date_rotate {
  my ($self, $schema, $table, $dt) = @_;

  my $rot_table = $self->date_rotate_name($table, $dt);
  my $tmp_table = "${table}_". $self->rand_str();

  local $SIG{INT};
  local $SIG{TERM};
  local $SIG{HUP};

  eval {
    $self->{dbh}->do(
      "CREATE TABLE `$schema`.`$tmp_table` LIKE `$schema`.`$table`"
    ) 
  };
  if($@) {
    $self->{errstr} = $@;
    croak("Unable to create new table $tmp_table");
  }

  eval {
    $self->{dbh}->do(
      "RENAME TABLE 
        `$schema`.`$table` TO `$schema`.`$rot_table`,
        `$schema`.`$tmp_table` TO `$schema`.`$table`"
      );
  };
  if($@) {
    $self->{errstr} = $@;
    croak("Failed to rename table to $rot_table, $tmp_table");
  }
  return $rot_table;
}

1;
