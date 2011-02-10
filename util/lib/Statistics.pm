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
package Statistics;
use strict;
use warnings FATAL => 'all';
use List::Util qw(sum min max reduce);
use Carp;

sub aggsum {
  my ($rows, @cols) = @_;
  my $vals = {};
  foreach my $row (@$rows) {
    my $vp = $vals;
    foreach my $col (@cols) {
      croak(__PACKAGE__ .': Unknown column: '. $col) unless(exists $row->{$col});
      if($col eq $cols[-1]) {
        $vp->{$row->{$col}} = 0 if(ref($vp->{$row->{$col}}) eq 'HASH');
        $vp->{$row->{$col}}++;
      }
      $vp = $vp->{$row->{$col}} ||= {};
    }
  }
  return $vals;
}

sub mean {
  my ($rows) = @_;
  $_ = sum(0, @$rows);
  $_ /= scalar(@$rows);
  return $_;
}

sub stdvar {
  my ($rows) = @_;
  my $m = mean($rows);
  my $s = scalar(@$rows);
  $_ = reduce { $a + ($b - $m)**2 } (0, @$rows);
  return 1/($s-1)*$_;
}

sub stddev {
  return sqrt(stdvar(@_));
}

=pod

=head1 NAME

SimpleStat

=head1 SYNOPSIS

A collection of simple subs to calculate common statistics for reports.

  # aggregate $arrayref by col1 and col2
  Statistics::aggsum($arrayref, 'col1', 'col2')

  # calculate the mean of a list of numbers
  Statistics::mean([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

  # Standard Variance
  Statistics::stdvar([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

  # Standard Deviation
  Statistics::stddev([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

=head1 METHODS

=over 8

=item C<aggsum($ar, $column[, $column2, ...])>

Takes an arrayref of hashrefs and produces a hashref.

  $VAR1 = [
    { name => 'manny', date => 123 },
    { name => 'fred', date => 125 },
    { name => 'bob', date => 123 },
    { name => 'manny', date => 123 }
  ];

  Statistics::aggsum($VAR1, 'name');
    => { 'manny' => 2, 'fred' => 1, 'bob' => 1 }

  Statistics::aggsum($VAR1, 'date');
    => { 123 => 3, 125 => 1 }

  Statistics::aggsum($VAR1, 'name', 'date');
    => { 'manny' => { 123 => 2 },
         'fred' => { 125 => 1 },
         'bob' =>  { 123 => 1 }
       }

=item C<mean($list)>

Calculates the mean. The average. Exactly what you'd expect.

=item C<stdvar($list)>

The standard variance. See Wikipedia.

=item C<stddev($list)>

The standard deviation. Again, Wikipedia.

=back

=cut

1;
