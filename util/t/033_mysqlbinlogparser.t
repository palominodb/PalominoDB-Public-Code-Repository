#!/usr/bin/env perl
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
