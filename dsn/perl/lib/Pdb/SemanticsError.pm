# SemanticsError.pm
# Copyright (C) 2012 PalominoDB, Inc.
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

package Pdb::SemanticsError;
use strict;
use warnings FATAL => 'all';
use base qw(Error);
use overload ('""' => 'stringify');

use constant Unknown => 'Unknown';
use constant UnknownCluster => 'UnknownCluster';
use constant ClusterMismatch => 'ClusterMismatch';
use constant EmptyDSN => 'EmptyDSN';
use constant PrimaryMismatch => 'PrimaryMismatch';
use constant FailoverMismatch => 'FailoverMismatch';

sub new {
  my ($class, $text, $errcode) = @_;
  my @args = ();
  local $Error::Depth = $Error::Depth +1;
  local $Error::Debug = 1;

  $class->SUPER::new(-text => $text, -value => $errcode, @args);
}
1;
