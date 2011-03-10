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
  $start ||= 0;

  if(not defined $table) {
    ($db, $table) = split /\./, $db;
  }
  if(not defined $index) {
    $index = $self->get_best_index($db, $table);
  }

  return ($self->walk_table_base(index => $index, size => $size, db => $db,
                                start => $start, callback => $cb,
                                table => $table, data => [@cb_data]))[0];
}

=pod

=head3 C<walk_table_base(%args)>

Accepts a hash of arguments to control how the table walk occurs.

Mandatory arguments:

C<index> - Which index to use (See L<get_best_index>).

C<callback> - Subroutine to call with row data.

C<size> - Size of bucket to iterate over.

C<db>, C<table> - Database and table to iterate over.

Optional arguments:

C<filter_clause> - Additional SQL to filter rows on (aka. "Where clause fragment").
Care should be taken to ensure that the additional filter makes proper use of indexes.

C<data> - Array ref of additional data to pass to the callback.

C<start> - What row to start at.

Returns the number of rows iterated over and the maximum index value examined.

=cut

sub walk_table_base {
  my ($self, %a) = @_;
  ## $last_idx is the global upper bound for the table
  ## $last_idx ensures that for a table that's currently growing
  ## we don't follow it indefinitely,
  my ($rows, $last_idx) = (0, 0);
  my ($idx_col) = ('');
  my $dbh = $self->{dsn}->get_dbh(1);

  for(qw(index callback size db table)) {
    croak("Missing required parameter: $_") unless(exists $a{$_});
  }

  $idx_col = $a{'index'}{'column'};
  $a{'filter_clause'} ||= '1=1';


  eval {
    ## Variables:
    ## $min_idx is the lower bound for the window into the table,
    ## $max_idx is the upper bound for the window,
    ## $cb is the callback (copied to local var for readability),
    ## @data will hold any extra parameters for the callback.
    my ($sth, $min_idx, $max_idx, $cb, $row, @data);

    # Start a transaction if one is not currently going
    $dbh->{AutoCommit} = 0;
    $cb = $a{'callback'};
    if(exists $a{'data'}) {
      @data = @{$a{'data'}};
    }
    $min_idx = $dbh->selectrow_array("SELECT MIN(`$idx_col`) FROM `$a{'db'}`.`$a{'table'}`");
    $last_idx = $dbh->selectrow_array("SELECT MAX(`$idx_col`) FROM `$a{'db'}`.`$a{'table'}`");
    $min_idx = $a{'start'} if(exists $a{'start'});
    $max_idx = $min_idx+$a{'size'};
    $sth = $dbh->prepare("SELECT * FROM `$a{'db'}`.`$a{'table'}` ".
                         "WHERE (`$idx_col` >= ? AND `$idx_col` <= ?) ".
                         "AND ($a{'filter_clause'})");

    do {
      $sth->execute($min_idx, $max_idx);
      while($row = $sth->fetchrow_hashref) {
        $rows++;
        &$cb($idx_col, $dbh, $min_idx, $max_idx, $row, @data);
      }
      $min_idx = $max_idx+1;
      $max_idx += $a{'size'};
      ## This ensures we don't walk past the range that we determined above
      ## with MAX($id_col)
      if($max_idx > $last_idx) {
        $max_idx = $last_idx;
      }
    } while($min_idx <= $last_idx);
    $dbh->commit;
    $dbh->{'AutoCommit'} = 0;
  };
  if($@) {
    $_ = "$@";
    ## Rollback is called here because the callback may be doing aribitrary
    ## transformations of the data.
    $dbh->rollback;
    croak($_);
  }

  return ($rows, $last_idx);
}

1;
