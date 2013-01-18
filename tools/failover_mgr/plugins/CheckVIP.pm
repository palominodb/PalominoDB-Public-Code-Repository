# CheckVIP.pm
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
 
package CheckVIP;
use strict;
use warnings FATAL => 'all';
use Exporter;
use Carp;
our @ISA = qw(FailoverPlugin);

sub post_verification {
  my ($self, $res, $pridsn, $faildsn) = @_;
  my @vips;
  @vips = @{$pridsn->{'vI'}->{'value'}} if(exists $pridsn->{'vI'});
  @vips = @{$faildsn->{'vI'}->{'value'}} if(exists $faildsn->{'vI'});
  foreach my $vip (@vips) {
    $::PLOG->d("Testing VIP:", $vip);
    my $vdsn = DSN->_create($pridsn);
    $vdsn->{'h'}->{'value'} = $vip;
    
    eval {
      my $dbh = $vdsn->get_dbh();
    };
    if($@) {
      $::PLOG->e("Unable to access MySQL through the VIP!");
    }
    else {
      $::PLOG->m('Successfully connected to MySQL through:', $vip);
    }
  }

}

1;
