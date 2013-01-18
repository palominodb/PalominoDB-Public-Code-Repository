# PruneRecords.pm
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

=pod

=head1 PURPOSE

This package is a plugin for pdb-munch to intelligently trim a dataset down
to a reasonable sample. So, given a table X, and tables Y and Z which
reference X, this plugin is designed to be given rows from X, and delete some
number of them until there are N left. At the same time, it also deletes any
corresponding records in Y and Z.

=head1 CONFIGURATION

This plugin makes use of some special keys in the table configuration.

They are:
  __prune.to        - How many records to keep in the table.
  __prune.refcolumn - The reference column.
  __prune.parent    - The the table that __prune.refcolumn refers to.

They must be set so that this plugin knows the mapping between tables.

Example table configuration:

  [table_X]
  __prune.to = 10_000
  id    = prune_records
  name  = obfuscate_name
  value = obfuscate_value
  
  [table_Y]
  __prune.refcolumn = table_X_id
  __prune.parent    = table_X
  other_value = swizzle_value

=cut

package PruneRecords;
use strict;
use warnings FATAL => 'all';

my $last_tbl = '';
my $last_idx = '';
my $n = 0;
my %o;

foreach my $tbl (keys %pdb_munch::conf) {
  next if($tbl eq '__connection__');
  my $to     = delete $pdb_munch::conf{$tbl}{'__prune.to'};
  my $parent = delete $pdb_munch::conf{$tbl}{'__prune.parent'};
  my $refcol = delete $pdb_munch::conf{$tbl}{'__prune.refcolumn'};
  $o{$tbl}{'to'} = int($to) if(defined $to);
  next if(not defined $parent);
  $o{$parent}{'children'} ||= [];
  push @{$o{$parent}{'children'}}, { tbl => $tbl, refcol => $refcol };
}

sub delete_row {
  my ($dbh, $coldata, $idxcol, $colname, $row) = @_;
  if($$row{$idxcol} ne $last_idx) {
    $n++;
    $last_idx = $$row{$idxcol};
  }
  if($last_tbl ne $pdb_munch::cur_tbl) {
    $n = 1;
    $last_tbl = $pdb_munch::cur_tbl;
    return $coldata;
  }
  if($n > $o{pruneto}) {
    $dbh->do("DELETE FROM $last_tbl WHERE $idxcol = ?", undef, $$row{$idxcol});
    foreach my $chld (@{$o{$last_tbl}{'children'}}) {
      $dbh->do("DELETE FROM $$chld{tbl} WHERE $$chld{refcol} = ?", undef, $$row{$idxcol});
    }
    return {};
  }
  return $coldata;
}

1;
