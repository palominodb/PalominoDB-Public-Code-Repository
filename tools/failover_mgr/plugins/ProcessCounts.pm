# ProcessCounts.pm
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
