#!/usr/bin/perl
use strict;
use warnings;
use Nagios::RemoteCmd;
use Getopt::Long;
use Data::Dumper;

my $host = undef;
my @services = qw();

GetOptions(
  "host=s" => \$host,
  "service|s=s" => \@services,
);


my $nagios = Nagios::RemoteCmd->new("https://develbox.linuxfood.net/nagios/", "brian", "system");

foreach my $s (@services) {
  $nagios->disable_notifications($host, $s);
}

