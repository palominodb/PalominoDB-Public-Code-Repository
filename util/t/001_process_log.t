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
use Test::More tests => 25;
use Fcntl;
my $pl;

BEGIN {
  # Ensure debugging is enabled.
  $ENV{'Pdb_DEBUG'} = 1;
  unlink('t/files/001_process_log2.t.log');
  unlink('t/files/001_process_log.t.log');
}

use ProcessLog;
{
  no warnings 'redefine';
  *ProcessLog::time = \&Overlord::time;
}

use IO::Handle;
use English qw(-no_match_vars);

sub get_line {
  my $n = shift;
  $n = 1 unless($n);
  open my $lh, '<', $pl->{'log_path'};
  while($lh->input_line_number < $n) {
    $_ = <$lh>;
  }
  close $lh;
  chomp($_);
  $_;
}

$pl = ProcessLog->new('001_process_log.t', 't/files/001_process_log.t.log', undef);
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

# stack_depth
is($pl->stack_depth(), 10, 'default stack depth');
is($pl->stack_depth(5), 10, 'set stack depth');
is($pl->stack_depth(), 5, 'read set stack depth');

my $pls = ProcessLog->new('001_process_log.t', 'syslog:LOCAL0', undef);
$pls->quiet(1);
eval {
  $pls->d('debug message');
  $pls->e('error message');
  $pls->i('notice message');
  $pls->m('info message');
  $pls->m("multi-line\nmessage\ntesting");
};
if($@) { fail('syslog messages'); diag($@); }
else { pass('syslog messages'); }

# Test prompts
use TestUtil;
my ($prompt_file) = get_test_data('processlog');
open my $prompt_fh, "<$prompt_file";

is($pl->p($prompt_fh, 'prompt: '), 'Unchecked input', 'unchecked input');

is($pl->p($prompt_fh, 'checked prompt: ', qr/^Checked input \d+$/),
  'Checked input 1', 'checked input 1');
is($pl->p($prompt_fh, 'checked prompt: ', qr/^Checked input \d+$/),
  'Checked input 2', 'checked input 2');

is($pl->p($prompt_fh, 'failed prompt: ', qr/^Correct input$/),
  'Correct input', 'repeat prompt till correct');
is($pl->p($prompt_fh, 'p: ', qr/^C$/, 'Y'), 'Y', 'default value');
is($pl->p($prompt_fh, 'p: ', qr/^C$/, 'Y'), 'C', 'default not used');

close($prompt_fh);

# Test changing the logpath to a new file
$pl->logpath('t/files/001_process_log2.t.log');
$pl->m('message 1');
$pl->m('message 2');
$pl->_flush();

diag('logpath: ', $pl->{'log_path'});
ok( -f "t/files/001_process_log2.t.log", "logpath changed to exists");
is(get_line(1), 'msg 0.000: message 1', "get line 1 from new log");
is(get_line(2), 'msg 0.000: message 2', "get line 2 from new log");

# Cleanup after ourselves
END {
  unlink('t/files/001_process_log.t.log');
  unlink('t/files/001_process_log2.t.log');
}
1;
