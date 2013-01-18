# TableAge.pm
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

package TableAge;
use strict;
use warnings FATAL => 'all';
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

sub older_than {
  my ($self, $tbl_age, $when) = @_;
  if(DateTime->compare($tbl_age, $when) == -1) {
    return 1;
  }
  return 0;
}

sub newer_than {
  my ($self, $tbl_age, $when) = @_;
  if(DateTime->compare($tbl_age, $when) == 1) {
    return 1;
  }
  return 0;
}

1;
