package Statistics;
use strict;
use warnings FATAL => 'all';
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

=pod

=head1 NAME

SimpleStat

=head1 SYNOPSIS

A collection of simple subs to calculate common statistics for reports.

  # aggregate $arrayref by col1 and col2
  Statistics::aggsum($arrayref, 'col1', 'col2')

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

=back

=cut

1;
