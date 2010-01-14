# The order of these tests is dependent.
# Do not add to the middle of this file unless you
# are prepared to update many tests.
package Overlord;
# This package is designed to overload 'troublesome'
# subroutines to make testing easier.
use strict;
sub time() {
  0;
}
1;

package main;
use strict;
use warnings;
use Test::More tests => 15;
use Fcntl;
use ProcessLog;

no warnings 'redefine';
*ProcessLog::time = \&Overlord::time;
use warnings;

use IO::Handle;
use English qw(-no_match_vars);

sub get_line {
  my $n = shift;
  $n = 1 unless($n);
  open my $lh, '<', '001_process_log.t.log';
  while($lh->input_line_number < $n) {
    $_ = <$lh>;
  }
  close $lh;
  chomp($_);
  $_;
}

# Make sure everything is peachy before we get started.
unlink('001_process_log.t.log');

my $pl = ProcessLog->new('001_process_log.t', '001_process_log.t.log', undef);
$pl->quiet(1);
ok($pl, 'instantiation');

ok($pl->name eq '001_process_log.t', 'process name');
ok($pl->runid, 'RunID generated');

$pl->start;
$pl->end;
$pl->m('test message 1');
$pl->ms('test stack message 1');
eval {
  $pl->ms('test stack message 2');
};
$pl->e('error message 1');
$pl->i('info message 1');
$pl->_flush;

ok(get_line(1) eq 'msg 0: BEGIN '. $pl->runid, 'start match');
ok(get_line(2) eq 'msg 0: END '. $pl->runid, 'end match');
ok(get_line(3) eq 'msg 0: test message 1', 'message 1');
ok(get_line(4) eq 'msg 0: test stack message 1', 'message 2');
ok(get_line(5) eq 'msg 0: No stack data available.', 'message 2s');

ok(get_line(6) eq 'msg 0: test stack message 2', 'message 3');
ok(get_line(7) eq 'msg 0: Stack trace:', 'message 3.1s');
ok(get_line(8) eq 'msg 0:  main  t/001_process_log.t:53  (eval)', 'message 3.2s');

ok(get_line(9) eq 'err main:56 0: error message 1', 'error 1');
ok(get_line(10) eq 'ifo 0: info message 1', 'info 1');

1;
