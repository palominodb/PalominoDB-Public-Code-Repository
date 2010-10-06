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
