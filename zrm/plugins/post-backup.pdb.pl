#!/usr/bin/perl
use strict;
use warnings;
use Getopt::Long;
use Nagios::RemoteCmd;

my $nagios_host = "";
my @nagios_services = qw();

my $nagios_user = "zrm";
my $nagios_pass = "zrm";
my $nagios_url  = "https://nagios.example.com/nagios/";
my $backup_directory = "";
my $backup_set = "";


GetOptions(
  'all-databases' => sub {},
  'databases=s' => sub {},
  'database=s' => sub {},
  'backup-directory=s' => \$backup_directory,
  'checksum-finished' => sub {},
  'checksum-pending' => sub {},
  'nagios-host=s' => \$nagios_host,
  'nagios-service|s=s' => \@nagios_services,
  'backup-set=s' => \$backup_set,
);


my $nagios = Nagios::RemoteCmd->new($nagios_url, $nagios_user, $nagios_pass);

# XXX: See pre-backup.pdb.pl for why we don't not cancel a downtime instead.
foreach my $s (@nagios_services) {
  $nagios->enable_notifications($nagios_host, $s);
}
