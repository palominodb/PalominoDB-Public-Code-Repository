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
use Test::More tests => 14;
use Fcntl;

BEGIN {
  # Ensure debugging is enabled.
  $ENV{'Pdb_DEBUG'} = 1;
}

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

is(get_line(1), 'msg 0.000: BEGIN '. $pl->runid, 'start match');
is(get_line(2), 'msg 0.000: END '. $pl->runid, 'end match');
is(get_line(3), 'msg 0.000: test message 1', 'message 1');
is(get_line(4), 'msg 0.000: test stack message 1', 'message 2');
is(get_line(5), 'msg 0.000: No stack data available.', 'message 2s');

is(get_line(6), 'msg 0.000: test stack message 2', 'message 3');
is(get_line(7), 'msg 0.000: Stack trace:', 'message 3.1s');
is(get_line(8), 'msg 0.000:  main  t/001_process_log.t:59  (eval)', 'message 3.2s');

is(get_line(9), 'err main:62 0.000: error message 1', 'error 1');
is(get_line(10), 'ifo 0.000: info message 1', 'info 1');

my $pls = ProcessLog->new('001_process_log.t', 'syslog:LOCAL0', undef);
$pls->quiet(1);
eval {
  $pls->d('debug message');
  $pls->e('error message');
  $pls->i('notice message');
  $pls->m('info message');
};
if($@) { fail('syslog messages'); diag($@); }
else { pass('syslog messages'); }


# Cleanup after ourselves
unlink('001_process_log.t.log');
1;
