# Copyright (c) 2009-2010, PalominoDB, Inc.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
# 
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
# 
#   * Neither the name of PalominoDB, Inc. nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
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
