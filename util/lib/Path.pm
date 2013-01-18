# Path.pm - removes files and directories given as arguments.
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

package Path;
use File::Find;

sub dir_empty {
  my $dir = shift;
  eval "use File::Find;";
  my $rmtree_sub = sub {
    if(-d $File::Find::name && $File::Find::name ne $dir) {
      rmdir $File::Find::name or die('rmtree: unable to remove directory '. $File::Find::name);
    }
    elsif($_ ne $dir) {
      unlink $File::Find::name or die('rmtree: unable to delete file '. $File::Find::name);
    }
    elsif($_ eq $dir) {
      return;
    }
    else {
      die('rmtree: unexpected error when attempting to remove ' . $File::Find::name);
    }
  };
  find( { wanted => $rmtree_sub, no_chdir => 1, bydepth => 1 }, $dir );

  return 0;
}
1;
