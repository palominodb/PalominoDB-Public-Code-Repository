use inc::Module::Install;

name "Pdb-DSN";
author q{PalominoDB Engineering <eng@palominodb.com>};
license 'bsd';
version 0.01;
all_from "lib/Pdb/DSN.pm";

build_requires 'Test::More';
recommends 'Crypt::SSLeay';
requires 'URI';
requires 'LWP';
requires 'DateTime';

WriteAll;
