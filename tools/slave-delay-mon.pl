#!/usr/bin/perl
# slave-delay-mon.pl - crepsucule 
# Copyright (C) 2013 PalominoDB, Inc.
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

use strict;
use warnings;

use Net::SNMP;
use Net::SSH::Perl;
use Data::Dumper;
use Getopt::Long;
use Pod::Usage;

use constant SNMP_PROC_INDEXES => '1.3.6.1.2.1.25.4.2.1.1';
use constant SNMP_PROC_NAMES   => '1.3.6.1.2.1.25.4.2.1.2';
use constant SNMP_PROC_PARAMS  => '1.3.6.1.2.1.25.4.2.1.5';

my $db_host;
my $db_user;
my $db_pass = undef;
my $snmp_community;
my $maatkit_path = "/usr/bin";
my $ssh_user;
my $ssh_key = "/root/.ssh/id_rsa";
my $ssh_pass;

my $delay = 3600;
my $interval = 60;

my $debug = 0;

# Collects and saves around options processing
my $script = "$0 " . join(' ', @ARGV);

GetOptions(
  "help" => sub { pod2usage(); },
  "debug" => \$debug,
  "db-host=s" => \$db_host,
  "db-user=s" => \$db_user,
  "db-pass=s" => \$db_pass,
  "ssh-key=s" => \$ssh_key,
  "ssh-user=s" => \$ssh_user,
  "ssh-pass=s" => \$ssh_pass,
  "community=s" => \$snmp_community,
  "maatkit-path=s" => \$maatkit_path,
  "delay" => \$delay,
  "interval" => \$interval
);

if(!$db_host || !$db_pass || !$db_user || !$ssh_user || !$delay || !$interval || !$snmp_community) {
  pod2usage(-msg => "Missing required option.");
}

if(!$ssh_key && $ssh_pass eq "") {
  pod2usage(-msg => "Either key or password for ssh Required.");
}

# Hides the passwords from ps
$script =~ s/(db|ssh)-pass(=?|\s+)\S+/$1-pass$2********/g;
$0 = $script;

my $s = Net::SNMP->session(
  -hostname => $db_host,
  -community => $snmp_community,
  -version => "snmpv2c"
);

my $r = $s->get_table( -baseoid => SNMP_PROC_NAMES);
my @perl_pids;
my $no_start = 0;
foreach my $k (sort keys %$r) {
  my @parts;
  @parts  = split(/\./, $k);
  push @perl_pids, $parts[-1] if ($r->{$k} eq "perl");
}

my @perl_oids;
foreach my $pid (@perl_pids) {
  print "[DEBUG]: Perl proc on remote machine: $pid\n";
  push @perl_oids, SNMP_PROC_PARAMS . ".$pid";
}

$r = $s->get_request( -varbindlist => \@perl_oids );

foreach my $k (sort keys %$r) {
  if($r->{$k} =~ /mk-slave-delay/) {
    print "[DEBUG]: Found mk-slave-delay running.\n";
    $no_start = 1;
  }
}

unless( $no_start ) {
  print "[DEBUG]: Didn't find running mk-slave delay.\n";
  my $ssh = Net::SSH::Perl->new($db_host, 'identity_files' => [$ssh_key]);

  $ssh->login($ssh_user, $ssh_pass);
  # XXX: This prevents us from capturing error text
  # XXX: Is there any way around it? -brian
  my ($out, $err, $exit) = $ssh->cmd("/usr/bin/perl $maatkit_path/mk-slave-delay --daemonize --delay $delay --interval $interval --password $db_pass --user $db_user localhost &>/dev/null &");
  if($exit != 0) {
    print STDERR "$out\n$err\nError starting mk-slave-delay!";
    exit($exit);
  }
}

__END__

=head1 NAME

slave-delay-mon.pl - Ensures that mk-slave-delay is running on a box.

=head1 SYNOPSIS

slave-delay-mon.pl [-h|--help] --db-host host --db-pass pass --db-user user --delay 1h --interval 5m

Option Summary:

    -h,--help     This help.

    --db-host     Host to run mk-slave-delay on (required).

    --db-user     User for mk-slave-delay (required).

    --db-pass     Password for mk-slave-delay (required).

    --delay       Delay target. See mk-slave-delay for more (required).

    --interval    How often mk-slave-delay should check (required).

    --ssh-user    User mk-slave-delay should run as (required).

    --ssh-pass    Password for ssh.

    --ssh-key     Key for ssh (Recommended over password).

    --community   SNMP v2 community name (required).

    --maatkit-path    Prefix to maatkit tools. (Default: /usr/bin).
