# Statistics.pm - A collection of simple subs to calculate common statistics for reports.
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
  return undef if(!scalar(@$rows));
  $_ = sum(0, @$rows);
  $_ /= scalar(@$rows);
  return $_;
}

sub stdvar {
  my ($rows) = @_;
  my $m = mean($rows);
  my $s = scalar(@$rows);
  $_ = reduce { no warnings 'once'; $a + ($b - $m)**2 } (0, @$rows);
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
