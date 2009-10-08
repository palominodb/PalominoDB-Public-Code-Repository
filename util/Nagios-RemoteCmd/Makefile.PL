use inc::Module::Install;

name     'Nagios-RemoteCmd';
all_from 'lib/Nagios/RemoteCmd.pm';
author   q{Brian Smith <brian@palominodb.com>};
license  'perl';

build_requires 'Test::More';
requires 'URI';
requires 'LWP';
requires 'Crypt::SSLeay';
requires 'DateTime';

auto_install;

WriteAll;

