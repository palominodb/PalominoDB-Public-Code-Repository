#!/usr/bin/perl
# find-blob-commit.pl
# Copyright (C) 2013 PalominoDB, Inc.
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

use 5.008;
use strict;
use Memoize;

die "usage: git-find-blob <blob> [<git-log arguments ...>]\n"
if not @ARGV;

my $obj_name = shift;

sub check_tree {
  my ( $tree ) = @_;
  my @subtree;

  {
    open my $ls_tree, '-|', git => 'ls-tree' => $tree
      or die "Couldn't open pipe to git-ls-tree: $!\n";

    while ( <$ls_tree> ) {
      /\A[0-7]{6} (\S+) (\S+)/
        or die "unexpected git-ls-tree output";
      return 1 if $2 eq $obj_name;
      push @subtree, $2 if $1 eq 'tree';
    }
  }

  check_tree( $_ ) && return 1 for @subtree;

  return;
}

memoize 'check_tree';

open my $log, '-|', git => log => @ARGV, '--pretty=format:%T %h %s'
  or die "Couldn't open pipe to git-log: $!\n";

while ( <$log> ) {
  chomp;
  my ( $tree, $commit, $subject ) = split " ", $_, 3;
  print "$commit $subject\n" if check_tree( $tree );
}
