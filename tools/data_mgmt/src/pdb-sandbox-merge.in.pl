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

package pdb_sandbox_merge;
use strict;
use warnings;

use Getopt::Long qw(GetOptionsFromArray :config no_ignore_case);
use Data::Dumper;
use List::Util;

use ProcessLog;
use IniFile;

$Data::Dumper::Indent = 1;

my $pl;

sub main {
  my @ARGV = @_;
  my %o;
  my $base;
  my @pids;
  $o{'ignore-db'} = ();
  $o{'parallel'} = 1;
  $o{'user'} = 'root';
  $o{'password'} = 'msandbox';
  @{$o{'ignore-db'}} = qw(mysql information_schema);
  GetOptionsFromArray(\@ARGV, \%o,
    'help|h',
    'user|u=s',
    'password|p=s',
    'ignore-db|i=s@',
    'include-mysql-db|I',
    'parallel|P!',
  );
  my @sandboxes = @ARGV;
  $pl = ProcessLog->new($0, '/dev/null', undef);

  if(@sandboxes < 2) {
    $pl->e('Cannot merge a single sandbox. That makes no sense.');
    return 1;
  }

  foreach my $sbox (@sandboxes) {
    unless( -d $sbox and -f "$sbox/my.sandbox.cnf" ) {
      $pl->e($sbox, 'does not look like a mysql sandbox.');
      return 1;
    }
  }

  $pl->d("options:\n", Dumper(\%o));

  # Shift off the first sandbox, where all the others will write.
  $base = shift @sandboxes;

  foreach my $sbox (@sandboxes) {
    my $pid=fork();
    if($pid == 0) {
      my %cfg = IniFile::read_config("$sbox/my.sandbox.cnf");
      if(not %cfg) {
        $pl->e("Unable to read $sbox/my.sandbox.cnf");
        exit(1);
      }
      my @dbs = qx|$sbox/use --user=$o{'user'} --password=$o{'password'} --batch --skip-column-names -e 'show databases'|;
      chomp(@dbs);
      @dbs = grep { !/mysql/ } @dbs;
      @dbs = grep { !/information_schema/} @dbs;
      @dbs = grep {
        my $a = $_;
        # That is: Make sure the db is not equal to any of the
        # listed ignores.
        any($a, @{$o{'ignore-db'}});
      } @dbs;
      $pl->d('dump command:', qq#$sbox/my sqldump --routines --no-autocommit --skip-add-drop-table --skip-add-drop-database --add-locks --extended-insert --quick --user=$o{'user'} --password=$o{'password'} --socket=$cfg{'mysqld'}{'socket'} --databases @dbs | $base/use --user=$o{'user'} --password=$o{'password'}#);
      exec("$sbox/my sqldump --routines --no-autocommit --skip-add-drop-table --skip-add-drop-database --add-locks --extended-insert --quick --user=$o{'user'} --password=$o{'password'} --socket=$cfg{'mysqld'}{'socket'} --databases @dbs | $base/use --user=$o{'user'} --password=$o{'password'}");
    }
    elsif(defined $pid and $pid > 0) {
      $pl->i("Spawned mysqldump for $sbox.");
      push @pids, $pid;
    }
    else {
      $pl->e("An error occurred while spawning a mysqldump for: $sbox");
      $pl->e("The merge has been aborted.");
      kill -15, @pids;
      1 while(wait != -1);
      return 1;
    }
    # If --noparallel, then wait for each.
    if(!$o{'parallel'}) { wait; }
  }

  # After we've spawned all our "threads",
  # Then we wait for them all.
  while(wait != -1) {
    if( ($? >> 8) != 0 ) {
      $pl->e("One of the loads did not complete successfully. It returned: $?");
      $pl->e("Aborting rest of dumps.");
      kill_all();
    }
  }

  return 0;
}

if( !caller ) { exit main(@ARGV); }

sub kill_all {
  kill -15, @_;
}

sub any {
  my $a = shift;
  $_ ne $a || return 0 for @_; 1;
}

1;
__END__

=head1 NAME

pdb-sandbox-merge - Merge N MysqlSandboxes into one.

=head1 RISKS

This section is here to inform you that this tool may have bugs.
In general, this tool should be safe, provided that you do not test
it out in production. At the time of this release, there are no known
bugs, but that does not mean there are none.

=head1 SYNOPSIS

pdb-sandbox-merge ~/sandboxes/msb1 ~/sandboxes/msb2 ... ~/sandboxes/msbN

=head1 ARGUMENTS

A list of paths to sandboxes should be provided either before or after
all options. pdb-sandbox-merge will exit if any of the provided paths
do not look like a mysql sandbox.

This tool only operates on local mysqlsandboxes. Support for remote mysql
servers is not currently a planned feature.

=head1 CONFLICT HANDLING

There are several ways conflicts during the merge can be handled.

=over 4

=item B<ignore>

Conflicts during the merge are simply ignored and data is trashed.

=item B<die>

The first conflict causes pdb-sandbox-merge to die. B<This is the default.> 

=back

At some later time, the following strategies will be implemented:

=over 4

=item B<database>

Databases with the same name are skipped in all sandboxes except the first.

=item B<table>

Tables in the same database with the same name are skipped in all but the first sandbox.

=item B<row>

The merge is aborted on the first row constraint failure. That is, an attempt to insert a duplicate primary key, or, a unique key fails.

=back

=head1 OPTIONS

=over 4

=item --user,-u

User across all sandboxes.

Default: root

=item --password,-p

Password accross all sandboxes.

Default: msandbox

=item --ignore-db,-i

May be specified many times. Causes the named database to not be merged.
The default is mysql, and additional calls to this option will add to that, not overwrite. See L<--include-mysql-db> to have mysql db merged.

Default: mysql

=item --include-mysql-db,-I

Causes the mysql db to be merged. The mysql db is not normally merged.

=item --parallel,-P

Do the merge in parallel. This is the default.

=back
