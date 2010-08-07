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
package TableIndexes;
use strict;
use warnings FATAL => 'all';
use Carp;

sub new {
  my ($class, $dsn) = @_;
  my $self = {};
  
  $self->{dsn} = $dsn;
  
  return bless $self, $class;
}

=pod

=head3 C<indexes($db, $table)>

Get all the for a table in exactly the order MySQL returns them.
You probably want L<sort_indexes>, instead, since that'll tell you
what indexes are "better".

=cut

sub indexes {
  my ($self, $db, $table) = @_;
  my $dbh = $$self{dsn}->get_dbh(1);
  my ($indexes, $columns);
  
  if(not defined $table) {
    ($db, $table) = split /\./, $db;
  }
  
  ## Determine what indexes are on a given table, and build a hashref of 'column name' => 'type'.
  ## This is used later when looping over the available indexes.
  $indexes = $dbh->selectall_arrayref("SHOW INDEXES FROM `$db`.`$table`", { Slice => {} });
  foreach my $col (@{$dbh->selectall_arrayref("SHOW COLUMNS FROM `$db`.`$table`", { Slice => {} });}) {
    $columns->{$col->{Field}} = $col->{Type};
  }
  
  $indexes = [
    map {
      my $key_name = $_->{'Key_name'};
      my $col_name = $_->{'Column_name'};
      my $col_type = $columns->{$col_name};
      $col_type =~ s/\(\d+\)//;
      $col_type = lc($col_type);
      my $key_type = undef;
      if($key_name ne 'PRIMARY') {
        if($_->{'Non_unique'}) {
          $key_type = 'key';
        }
        else {
          $key_type = 'unique';
        }
      }
      else {
        $key_type = 'primary';
      }
      { 'name' => $_->{'Key_name'}, 'column' => $_->{'Column_name'}, 'key_type' => $key_type, 'column_type' => $col_type }
    } @$indexes
  ];
  
  return $indexes;
}

=pod

=head3 C<sort_indexes($db, $table)>

Get a list of index -> column pairs sorted from best to worst.

The assumption is that you're going to walk the table with one or more
of these indexes. Naturally, this isn't always what you want to do.
The way it selects the best index is in this order:

=over 8

=item PRIMARY (integer)

Only the first column of a PK is returned.

=item PRIMARY (timestamp)

=item UNIQUE (integer)

=item UNIQUE (timestamp)

=item KEY (integer)

=item KEY (timestamp)

=item ABORTS

If no indexed column of the types above exist, it croaks() with "No suitable indexed column available".

=back

=cut


  
## This implements the plumbing for the index sort later on. It composes a keytype-columntype
## string, locates it in the @index_priority array, and returns the index.
## It is intentionally not documented, because it's not meant to be used outside sort_indexes
sub i_col_typ {
  my $x = shift;
  my $i = 0;

  ## Used for sorting the various keytype-columntype pairs.
  ## The higher (closer to 0) a pair is in this array, the better it is.
  my @index_priority = ('primary-int', 'primary-timestamp',
                      'unique-int', 'unique-timestamp',
                      'key-int', 'key-timestamp');
  my $kt = $x->{'key_type'};
  my $ct = $x->{'column_type'};
  
  $i++ while($index_priority[$i] and $index_priority[$i] !~ /^${kt}-${ct}$/);
  return -1 if(! $index_priority[$i]);
  return $i;
}

sub sort_indexes {
  my ($self, $db, $table) = @_;
  my $dbh = $$self{dsn}->get_dbh(1);
  my ($indexes, $columns);
  
  if(not defined $table) {
    ($db, $table) = split /\./, $db;
  }
  
  $indexes = [
    sort {
       i_col_typ($a) <=> i_col_typ($b);
    }
    grep { i_col_typ($_) >= 0 } @{$self->indexes($db, $table)}
  ];
  croak("No suitable index found") if(!@$indexes);
  return $indexes;
}

=pod

=head3 C<get_best_indexed_column($db, $table)>

Try to find the "best" column in a table for issuing
SELECTs/UPDATEs against. If C<$table> is undef, then
C<$db> is split on C<'.'> and used as both db and table.

This uses sort_indexes and selects the top candidate.

=cut

sub get_best_index {
  my ($self, $db, $table) = @_;
  my $dbh = $$self{dsn}->get_dbh(1);
  my ($indexes, $columns, @index_type);
  
  if(not defined $table) {
    ($db, $table) = split /\./, $db;
  }
  return $self->sort_indexes($db, $table)->[0];

}

=pod

=head3 C<walk_table($index, $size, $start, $cb, $db, $table, @cb_data)>

If C<$index> is defined, then walk up that index, otherwise,
use L<get_best_index> and walk up that one.

C<$size> defines approximately how many rows at a time should
be fetched at once.

C<$start> start the walk from a row other than the first.

C<$cb> is a coderef to call with: $index_column, $dbh, $min_id, $max_id, $row_data, @cb_data
Where, min_id and max_id compose the range of the index currently being queried.

=cut

sub walk_table {
  my ($self, $index, $size, $start, $cb, $db, $table, @cb_data) = @_;
  my $dbh = $self->{dsn}->get_dbh(1);
  my ($sth, $min_idx, $max_idx, $last_idx, $rows);
  my $row;
  $start ||= 0;
  $rows = 0;
  
  if(not defined $table) {
    ($db, $table) = split /\./, $db;
  }
  if(not defined $index) {
    $index = $self->get_best_index($db, $table);
  }
  eval {
    # Start a transaction if one is not currently going
    $dbh->begin_work if($dbh->{AutoCommit});
    $min_idx = $dbh->selectrow_array("SELECT `$index->{'column'}` FROM `$db`.`$table` LIMIT 1");
    $last_idx = $dbh->selectrow_array("SELECT MAX(`$index->{'column'}`) FROM `$db`.`$table` LIMIT 1");
    $min_idx = $start if($start);
    $max_idx = $min_idx+$size;
    $sth = $dbh->prepare("SELECT * FROM `$db`.`$table` WHERE `$index->{'column'}` >= ? AND `$index->{'column'}` <= ?");
  
    do {
      $sth->execute($min_idx, $max_idx);
      while($row = $sth->fetchrow_hashref) {
        $rows++;
        &$cb($index->{'column'}, $dbh, $min_idx, $max_idx, $row, @cb_data);
      }
      $min_idx = $max_idx+1;
      $max_idx += $size;
    } while($min_idx < $last_idx);
  };
  if($@) {
    ## Rollback is called here because the callback may be doing aribitrary
    ## transformations of the data.
    $dbh->rollback;
    croak($@);
  }
  
  $dbh->commit;
  ## Restore AutoCommit to 0 after we've walked the table.
  $dbh->begin_work if($dbh->{AutoCommit});
  return $rows;
}

1;
