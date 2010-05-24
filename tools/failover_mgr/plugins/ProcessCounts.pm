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
package ProcessCounts;
use strict;
use warnings FATAL => 'all';
use ProcessLog;
use Exporter;
use DBI;
use MysqlSlave;
use Statistics;
use Carp;
use FailoverPlugin;
our @ISA = qw(FailoverPlugin);

sub new {
  my $class = shift;
  my $opts = shift;
  return bless $class->SUPER::new($opts), $class;
}

sub user_count {
  my $dsn = shift;
  my $dbh = $dsn->get_dbh(1);
  # Get aggregate by user
  my $cnts = Statistics::aggsum(
    $dbh->selectall_arrayref(qq|SHOW PROCESSLIST|, { Slice => {} }),
    'User'
  );
  my $cnt_str = $dsn->get('h') . " users: ";
  foreach my $u (sort keys(%$cnts)) {
    $cnt_str .= "${u}: $cnts->{$u}, ";
  }
  chop($cnt_str); chop($cnt_str);
  $::PLOG->i($cnt_str);
}

sub pre_verification {
  my $self = shift;
  my $pridsn = $_[0];
  my $faildsn = $_[1];

  # Report on user connections and prompt to continue
  for(@_) { user_count($_); }
  if($::PLOG->p('Continue [Yes/No]?', qr/^(Yes|No)$/i) eq 'No') {
    croak('Aborting failover');
  }
}

sub post_verification {
  my $self = shift;
  my $status = shift;
  for(@_) { user_count($_); }
}

1;
