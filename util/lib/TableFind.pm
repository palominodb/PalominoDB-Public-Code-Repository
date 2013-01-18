# TableFind.pm - Finds tables matching a set of patterns
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

package TableFind;
use strict;
use warnings FATAL => 'all';
use Carp;
use DateTime;
use DateTime::Format::Strptime;
use DSN;

sub new {
  my ($class, $dsn) = @_;
  my $self = {};
  croak("Need D key") unless($dsn->get('D'));
  $self->{dsn} = $dsn;
  return bless $self, $class;
}

sub find {
  my ($self, @preds) = @_;
  if((scalar @preds % 2) != 0) {
    croak("Uneven number of predicates and arguments");
  }
  my @res;
  my $tbl_stat = $self->{dsn}->get_dbh(1)->selectall_arrayref(
    "SHOW TABLE STATUS FROM `". $$self{dsn}->get('D') ."`",
    { Slice => {} }
  );
  TABLE:
  foreach my $tbl (@$tbl_stat) {
    for(my $i=0; $i<$#preds; $i+=2) {
      no strict 'refs';
      my $pred = $preds[$i];
      my @pred_args = (ref($preds[$i+1]) eq 'ARRAY'
                         ? @{$preds[$i+1]}
                           : ($preds[$i+1]));
      my $result = 0;
      eval {

        $result = &{"PREDICATE_$pred"}($self, $tbl, @pred_args);
      };
      if($@ =~ /^Undefined subroutine.*/) {
        croak("Unknown predicate $pred");
      }
      elsif($@) {
        croak($@);
      }
      # Test the predicate result, and if it returns a
      # false value (0, undef, ''), skip to the next table.
      next TABLE unless($result);
    }
    push @res, $tbl;
  }
  return @res;
}

sub PREDICATE_name {
  my ($self, $tbl, $pred_args) = @_;
  return $tbl->{Name} =~ $pred_args;
}

sub PREDICATE_engine {
  my ($self, $tbl, $pred_args) = @_;
  return lc($tbl->{Engine}) eq lc($pred_args);
}

sub PREDICATE_row_format {
  my ($self, $tbl, $pred_args) = @_;
  return lc($tbl->{Row_format}) eq lc($pred_args);
}

sub PREDICATE_smaller_than {
  my ($self, $tbl, $pred_args) = @_;
  return $tbl->{Data_length}+$tbl->{Index_length} < $pred_args;
}

sub PREDICATE_greater_than {
  my ($self, $tbl, $pred_args) = @_;
  return $tbl->{Data_length}+$tbl->{Index_length} > $pred_args;
}


sub PREDICATE_agebyname {
  my ($self, $tbl, $pred_args) = @_;
  my $tbl_age = undef;
  my $result  = 0;
  my $older_than = $$pred_args{'older_than'};
  my $newer_than = $$pred_args{'newer_than'};
  my $eq_to      = $$pred_args{'eq_to'};
  my $fmt = $$pred_args{'pattern'};

  if( not ref($fmt) ) {
    $fmt = DateTime::Format::Strptime->new(pattern => $fmt,
                                           time_zone => 'local');
  }
  $tbl_age = $fmt->parse_datetime($tbl->{Name});
  if( $tbl_age ) {
    if($older_than and DateTime->compare($tbl_age, $older_than) == -1) {
      return 1;
    }
    if($newer_than and DateTime->compare($tbl_age, $newer_than) == 1) {
      return 1;
    }
    if($eq_to and DateTime->compare($tbl_age, $eq_to) == 0) {
      return 1;
    }
  }
  return 0;
}

1;

=pod

=head1 NAME

TableFind - Find tables matching a set of patterns.

=head1 SYNOPSIS

TableFind finds tables matching a variety of patterns.

  # $dsn == a DSN object from: "h=db,u=user,p=pass,D=test"
  my $tf = TableFind->new($dsn);
  
  # Find all tables matching /some.*name_(\d+)/
  $tf->find(name => qr/some.*name_(\d+)/);

  # Find all tables newer than 2010-11-01
  $tf->find(agebyname => { pattern => 'sometbl_%Y%m%d',
                           newer_than => DateTime->new(year => 2010,
                                                       month => 11,
                                                       day => 1)
                         });
  # Find all tables starting with somename_ using InnoDB.
  $tf->find(name => qr/^somename_/, engine => 'innodb');

TableFind will only return the set of tables that match all predicates.
i.e., the conditions are ANDed together.

=cut
