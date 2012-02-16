use strict;
use warnings FATAL => 'all';
use Test::More tests => 5;

BEGIN {
  use_ok('Statistics');
}

my $VAR1 = [
  { name => 'bob', date => 123, activity => 3, age => 20 },
  { name => 'stan', date => 124, activity => 5, age => 23 },
  { name => 'fred', date => 121, activity => 5, age => 20 },
  { name => 'charlie', date => 125, activity => 4, age => 28 },
  { name => 'bob', date => 123, activity => 3, age => 20 },
  { name => 'charlie', date => 125, activity => 5, age => 28 },
  { name => 'bob', date => 124, activity => 3, age => 20 },
];

is_deeply(Statistics::aggsum($VAR1, 'name'),
  { 'bob' => 3, 'charlie' => 2, 'stan' => 1, 'fred' => 1 },
  'single column');

is_deeply(Statistics::aggsum($VAR1, 'name', 'date'),
 {'charlie' => {'125' => 2},
   'bob' => {'124' => 1,'123' => 2},
   'fred' => {'121' => 1},
   'stan' => {'124' => 1}
 },
  'two column');

is_deeply(Statistics::aggsum($VAR1, 'activity', 'date', 'name'),
 {'4' => 
   {'125' => {'charlie' => 1}},
  '3' => {'124' => {'bob' => 1},
          '123' => {'bob' => 2}},
  '5' => {'124' => {'stan' => 1},
          '125' => {'charlie' => 1},
          '121' => {'fred' => 1}}
 },
 'three column');

eval {
  Statistics::aggsum($VAR1, 'email');
};
like($@, qr/Statistics: Unknown column: email/, 'croak on unknown column');
