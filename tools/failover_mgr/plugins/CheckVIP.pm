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
