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
