use inc::Module::Install;

name "Pdb-DSN";
author q{Brian Smith <brian@palominodb.com>};
license 'perl';
version 0.01;
all_from "lib/Pdb/DSN.pm";

build_requires 'Test::More';
recommends 'Crypt::SSLeay';
requires 'URI';
requires 'LWP';
requires 'DateTime';

WriteAll;
