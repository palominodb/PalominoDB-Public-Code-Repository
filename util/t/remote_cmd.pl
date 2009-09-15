#!/usr/bin/perl
use strict;
use warnings;
use Nagios::RemoteCmd;
use Data::Dumper;


my $nagios = Nagios::RemoteCmd->new("https://develbox.linuxfood.net/nagios/", "brian", "system");

$nagios->disable_notifications("testdb2.i.linuxfood.net", "PING", "babies.", "crybabies");
