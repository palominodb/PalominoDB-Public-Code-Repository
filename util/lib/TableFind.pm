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
use TableAge;
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
	my @res;
	my $tbl_stat = $self->{dsn}->get_dbh(1)->selectall_arrayref(
	       "SHOW TABLE STATUS FROM `". $$self{dsn}->get('D') ."`",
	       { Slice => {} }
	   );
	foreach my $tbl (@$tbl_stat) {
  	for(my $i=0; $i<$#preds; $i++) {
	  	no strict 'refs';
		  my $pred = $preds[$i];
		  my $pred_args = $preds[$i+1];
		  eval {
  		  if(ref($pred_args) eq 'ARRAY') {
  		    &{"PREDICATE_$pred"}($self, $tbl, @$pred_args) and push @res, $tbl;
  		  }
  		  else {
  		  	&{"PREDICATE_$pred"}($self, $tbl, $pred_args) and push @res, $tbl;
  		  }
		  };
		  if($@ =~ /^Undefined subroutine.*/) {
		    croak("Unknown predicate $pred");
		  }
		  elsif($@) {
		    croak($@);
		  }
	  }
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

1;

=pod

=head1 NAME

TableFind - Find tables matching a pattern.

=head1 SYNOPSIS

TableFind finds tables matching a variety of patterns.

  # $dsn == a DSN object from: "h=db,u=user,p=pass,D=test"
  my $tf = TableFind->new($dsn);
  
  # Find all tables matching /some.*name_(\d+)/
  $tf->find(name => qr/some.*name_(\d+)/);

=cut