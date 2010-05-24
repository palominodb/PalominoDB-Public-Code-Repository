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
