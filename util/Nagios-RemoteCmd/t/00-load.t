#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'Nagios::RemoteCmd' );
}

diag( "Testing Nagios::RemoteCmd $Nagios::RemoteCmd::VERSION, Perl $], $^X" );
