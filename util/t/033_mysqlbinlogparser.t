#!/usr/bin/env perl
# 033_mysqlbinlogparser.t
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

use strict;
use warnings FATAL => 'all';
use TestUtil;
use Data::Dumper;
use MIME::Base64;
$Data::Dumper::Indent = 3;
use Test::More qw(no_plan); #tests => 2*get_test_data('binlogs', '[0-9]*')+2;

BEGIN {
  use_ok('MysqlBinlogParser');
}

is(scalar get_test_data('binlogs', 'txt'),
   scalar get_test_data('binlogs', '[0-9]*'),
   'Have validation data for all binlogs');

foreach my $binlog (get_test_data('binlogs', '[0-9]*')) {
  diag($binlog);
  my $i = 0;
  my $binlp = new_ok('MysqlBinlogParser' => [$binlog]);
  (undef, $_) = split('\.', $binlog);
  $_ = get_files_dir() . '/binlogs/'. $_ . '.txt';
  diag($_);
  my $validation = eval "". slurp($_) . ";";
  if(not defined $validation) {
    $validation = [];
  }
  diag("validation data:\n" . Dumper($validation));
  delete $binlp->{header}->{create_timestamp};
  delete $binlp->{header}->{ts};
  diag("header:\n". Dumper($binlp->{header}));
  is_deeply($binlp->{header}, shift @$validation, 'header parsed');
  eval {
    my @events;
    while($_ = $binlp->read()) {
      delete $$_{ts}; # this will change every time binlogs are generated
      is_deeply($_, $validation->[$i], "$binlog event $i");
      push @events, $_;
      $i++;
    }
    diag(Dumper(\@events));
  };
  is($@, '', 'no exceptions while reading');
}
