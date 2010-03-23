#!/usr/bin/env perl
use strict;
use warnings;

# ###########################################################################
# ProcessLog package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessLog package
# ###########################################################################

# ###########################################################################
# IniFile package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End IniFile package
# ###########################################################################

# ###########################################################################
# RObj package GIT_VERSION
# ###########################################################################
# ###########################################################################
# End RObj package
# ###########################################################################

package pdb_master;
use strict;
use warnings;

use Getopt::Long qw(GetOptionsFromArray :config no_ignore_case);

use ProcessLog;
use IniFile;
use RObj;

sub main {
  my @ARGV = @_;
}

if(!caller) { main(@ARGV); }

sub parse_hostref {
  my $ref = shift;
  my ($host, $user, $path);
  if($ref =~ /^(.+?)\@(.+?):(.+?)$/) {
    $host = $2;
    $user = $1;
    $path = $3;
  }
  elsif($ref =~ /^(.+?)\@(.+?)$/) {
    $user = $1;
    $host = $2;
    $path = undef;
  }
  elsif($ref =~ /^(.+?):(.+?)$/) {
    $host = $1;
    $path = $2;
  }
  elsif($ref =~ /^(.+?)$/) {
    $host = $1;
  }
  return undef if(!$host or $host eq '');
  return wantarray ? ($user, $host, $path) : [$user, $host, $path];
}

sub get_mycnf {
  my $path = shift;
  unless($path) {
    if($^O eq 'linux') {
      if(-f "/etc/debian_version") {
        $path = "/etc/mysql/my.cnf";
      }
      elsif(-f "/etc/redhat-release") {
        $path = "/etc/my.cnf";
      }
    }
    elsif($^O eq 'darwin') {
      # TODO
      $path = "/Users/linuxfood/sandboxes/msb_5_1_41/my.sandbox.cnf";
    }
    elsif($^O eq 'freebsd') {
      $path = "/etc/my.cnf";
    }
  }

  return $path ? IniFile::read_config($path) : undef;
}

1;
__END__

=head1 NAME

pdb-master - build a master-master cluster from a mysqlsandbox

=head1 SYNOPSIS

pdb-master <sandbox path> <[user@]host[:my.cnf path]>+

=head1 ARGUMENTS

After options, the arguments to pdb-master are a path to a sandbox.
This must be a filesystem path. Followed by one or more host references.
You may not mix the sandbox path, and host references. The tool will fail if
the first argument is not a directory.

Host references are an optional username followed by an at sign C<@>,
followed by a mandatory hostname, followed by an optional colon C<:> and
a path to a my.cnf file on the remote host.

Example host references:

C<root@db1> , C<db2:/etc/prd/my.cnf> , C<db3> , C<mysql@db4:/etc/my.cnf>

pdb-master tries to be as smart as possible in it's operation.
So, it will autodetect standard locations for RedHat and Debian Linux
distributions, and FreeBSD. Meaning that specifying the my.cnf path
is not usually needed.

=head1 REQUIREMENTS

Where possible this tool inline's its requirements much like
L<http://code.google.com/p/maatkit/> in order to reduce external dependencies.

At the time of writing this tool needs the following external commands
available in the $PATH: ssh, mysql, mysqldump, scp, and tar.

For SSH access to the remote hosts, only public key authentication is allowed.
This is done both for security and to accomodate Debian. There is a Net::SSH::Perl
module but Debian refuses to package it because one of its dependencies is
"hard" to pacakge.

=head1 OPTIONS

=over 4

=item --help,-h

This help.

=item --dry-run,-n

Report on, but do not execute, actions that would be taken.

=item --ssh-key,-i

SSH key to use when connecting to remote hosts.
If not specified it is assumed that an L<ssh-agent(1)> is running,
or that .ssh/config has been setup appropriately.

Default: none

=item --save,-S

Preserve a database on the remote machine.
This option can be specified multiple times to save many databases.
See L<--save-method> for details on how databases are saved.

Default: mysql

=item --save-method

=over 4

=item auto

This method auto selects one of the below methods based on the table types
in the database.

=item dump

This method does a mysqldump of the database. It's the only way to backup
databases that have either all InnoDB tables, or a mix between InnoDB and others.

=item copy

This method only works when the database contains only tables that are MyISAM and CSV.
This method breaks when there are InnoDB tables.

=back

Default: auto

=back
