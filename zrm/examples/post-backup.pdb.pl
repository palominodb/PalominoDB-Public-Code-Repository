#!/usr/bin/perl
# 
# Post-backup hook script for mysql-zrm backup which, when enabled,
# automatically re-enables notifications for the database hosts getting backed
# up.
#
# Copyright (C) 2012 PalominoDB, Inc.
#
# You may contact the maintainers at eng@palominodb.com.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

use lib '/usr/share/mysql-zrm/plugins';
use strict;
use warnings;
use Getopt::Long;
use Nagios::RemoteCmd;
use Text::ParseWords;

my $nagios_host = "";
my @nagios_services = qw();

my $nagios_user = "zrm";
my $nagios_pass = "zrm";
my $nagios_url  = "https://nagios.example.com/nagios/";
my $backup_directory = "";
my $backup_set = "";

@ARGV = shellwords(@ARGV) unless(scalar @ARGV > 1);

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
if($nagios_host ne "") {
  print "Enabling Nagios alerts for $nagios_host\n";
  foreach my $s (@nagios_services) {
    print "Enabling $s service.\n";
    $nagios->enable_notifications($nagios_host, $s);
  }
}
