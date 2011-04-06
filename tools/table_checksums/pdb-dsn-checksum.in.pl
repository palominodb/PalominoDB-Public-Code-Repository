#!/usr/bin/env perl
# Copyright (c) 2009-2010, PalominoDB, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#
#   * Neither the name of PalominoDB, Inc. nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

use strict;
use warnings FATAL => 'all';

# ###########################################################################
# ProcessLog package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End ProcessLog package
# ###########################################################################

# ###########################################################################
# YAMLDSN package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End YAMLDSN package
# ###########################################################################

# ###########################################################################
# Lockfile package FSL_VERSION
# ###########################################################################
# ###########################################################################
# End Lockfile package
# ###########################################################################

package pdb_dsn_checksum;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Data::Dumper;
use Getopt::Long qw(:config no_ignore_case);
use Pod::Usage;
use List::Util qw(max);

use YAMLDSN;
use ProcessLog;

my $pl = undef;
my $dsnuri = undef;
my $loguri = "$0.log";
my $user = undef;
my $password = undef;
my $repl_table = undef;
my @checksum_masters = ();
my $central_dsn = undef;
my $only_report = 0;
my $pretend = 0;

my $allow_slave_lag = 0;

my @global_ignore_tables = ('mysql.slow_log', 'mysql.general_log');
my @global_ignore_databases = ();
my $mk_table_checksum_path = '/usr/bin/mk-table-checksum';

my %checksum_master_opts;

my $csum_dsn = undef;
my $csum_dbh = undef;

my $default_chunk_size = 50_000;
my $cluster = undef;
my $lock = undef;
my $lock_timeout = undef;

my $dp;

sub main {
  my (@ARGV) = @_;
  my $program_lock;
  GetOptions(
    'help|?'  => sub { pod2usage(-verbose => 99); },
    'dsn|d=s' => \$dsnuri,
    'user|u=s' => \$user,
    'logfile|L=s'    => \$loguri,
    'password|p=s' => \$password,
    'store|S=s' => \$central_dsn,
    'replicate-table|R=s' => \$repl_table,
    'allow-slave-lag|a' => \$allow_slave_lag,
    'only-report' => \$only_report,
    'pretend' => \$pretend,
    'mk-table-checksum-path=s' => \$mk_table_checksum_path,
    'cluster=s' => \$cluster,
    'lock=s' => \$lock,
    'lock-timeout=i' => \$lock_timeout
  ) or die('Try --help');

  unless(defined($central_dsn) and defined($dsnuri) and defined($user) and defined($password) and defined($repl_table)) {
    pod2usage(-message => '--dsn, --user, --password, --store, and --replicate-table are all required.', -verbose => 99);
    return 1;
  }

  $pl = ProcessLog->new($0, $loguri, undef);
  $pl->start;

  if($lock) {
    eval {
      $program_lock = Lockfile->get($lock, $lock_timeout);
    };
    if($@) {
      $pl->e("$@");
      $pl->end();
      return 1;
    }
  }

  $ENV{MKDEBUG} = exists $ENV{MKDEBUG} ? $ENV{MKDEBUG} : $pretend;
  eval "require '$mk_table_checksum_path'";
  if($EVAL_ERROR) {
    $pl->e($EVAL_ERROR);
    return 1;
  }

  my $dsn = YAMLDSN->new($dsnuri);
  ## This DSNParser is not the PalominoDB one, but the Maatkit one.
  $dp       = DSNParser->new({key => 't', 'desc' => 'Table to write to', copy => 0});
  $csum_dsn = $dp->parse($central_dsn);
  $csum_dbh = $dp->get_dbh($dp->get_cxn_params($csum_dsn));

  foreach my $c ($dsn->get_all_clusters()) {
    next if( defined($cluster) and $cluster ne $c );
    $pl->d("CLUSTER:", $c);
    my $s = $dsn->cluster_primary($c);
    $pl->i("CLUSTER ($c) PRIMARY:", $s);
    push(@checksum_masters, $s) if($dsn->server_checksum($s));
    if(defined(my $opts = $dsn->server_checksum_options($s))) {
      $pl->i("CLUSTER ($c) CHECKSUM OPTIONS:", Dumper($opts));
      if($opts->{ignore_tables}) {
        $checksum_master_opts{$s}{ignore_tables} = $opts->{ignore_tables};
      }
      if($opts->{ignore_databases}) {
        $checksum_master_opts{$s}{ignore_databases} = $opts->{ignore_databases};
      }
      if($opts->{chunk_size}) {
        $checksum_master_opts{$s}{chunk_size} = $opts->{chunk_size};
      }
      if($opts->{tables}) {
        $checksum_master_opts{$s}{tables} = $opts->{tables};
      }
      if($opts->{since}) {
        $checksum_master_opts{$s}{since} = $opts->{since};
      }
    }
  }

  unless ($only_report) {
    foreach my $cm (@checksum_masters) {
      $pl->i("CHECKSUMMING CLUSTER PRIMARY:",$cm);
      my $c_size = undef;
      my @ignore_databases = (@{$checksum_master_opts{$cm}{ignore_databases} || []}, @global_ignore_databases);
      my @ignore_tables    = (@{$checksum_master_opts{$cm}{ignore_tables} || []}, @global_ignore_tables);
      my %do_tables        = %{$checksum_master_opts{$cm}{tables} || {}};
      if(exists $checksum_master_opts{$cm}{chunk_size}) {
        $c_size = $checksum_master_opts{$cm}{chunk_size};
      }
      else {
        $c_size = $default_chunk_size;
      }
      my $since            = $checksum_master_opts{$cm}{since};
      my $ignore_indexes   = 0;

      # Run through individual tables if 'tables:'
      # key exists
      if(scalar %do_tables) {
        foreach my $t (keys %do_tables) {
          ## Per-table config values. May be just a where clause (plain string),
          ## or could be a hash-ref, in which case it contains option overrides.
          my $tv = $do_tables{$t};
          my $where = undef;

          if(ref($tv) eq 'HASH') {
            $where = $$tv{'where'};
            if(exists $$tv{'chunk_size'}) {
              $c_size = $$tv{'chunk_size'};
            }
            if(exists $$tv{'since'}) {
              $since = $$tv{'since'};
            }
            if(exists $$tv{'no_use_index'}) {
              $ignore_indexes = $$tv{'no_use_index'};
            }
          }
          elsif(!ref($do_tables{$t})) {
            $where = $tv;
          }
          else {
            $pl->ed($cm. ' checksum_options.tables contained an invalid entry '.
                    'for '. $t, 'Not a string or key value list.');
          }

          my @mk_args = (
            $pretend ? ('--explain') : (),
            !$ENV{Pdb_DEBUG} ? ('--quiet') : (),
            '--create-replicate-table',
            '--empty-replicate-table',
            '--replicate', $repl_table,
            '--user', $user,
            '--password', $password,
            scalar @ignore_databases ? ('--ignore-databases', join(',', @ignore_databases)) : (),
            scalar @ignore_tables ? ('--ignore-tables', join(',',@ignore_tables)) : (),
            '--tables', $t,
            $since ? ('--since', $since) : (),
            $c_size ? ('--chunk-size', $c_size) : (),
            $ignore_indexes ? ('--no-use-index') : (),
            $where ? ('--where', $where) : (),
            $cm
          );
          run_mk_checksum($cm, @mk_args);
        }
      }
      else {
        my @mk_args = (
          $pretend ? ('--explain') : (),
          !$ENV{Pdb_DEBUG} ? ('--quiet') : (),
          '--create-replicate-table',
          '--empty-replicate-table',
          '--replicate', $repl_table,
          '--user', $user,
          '--password', $password,
          scalar @ignore_databases ? ('--ignore-databases', join(',', @ignore_databases)) : (),
          scalar @ignore_tables ? ('--ignore-tables', join(',',@ignore_tables)) : (),
          $since ? ('--since', $since) : (),
          $c_size ? ('--chunk-size', $c_size) : (),
          $ignore_indexes ? ('--no-use-index') : (),
          $cm
        );
        run_mk_checksum(@mk_args);
      }
    }
  }

  (my $sql = <<"EOF") =~ s/\s+/ /gm;
   SELECT host, db, tbl, chunk, boundaries,
      COALESCE(this_cnt-master_cnt, 0) AS cnt_diff,
      COALESCE(
         this_crc <> master_crc OR ISNULL(master_crc) <> ISNULL(this_crc),
         0
      ) AS crc_diff,
      this_cnt, master_cnt, this_crc, master_crc
   FROM `$csum_dsn->{D}`.`$csum_dsn->{t}`
   WHERE master_cnt <> this_cnt OR master_crc <> this_crc
   OR ISNULL(master_crc) <> ISNULL(this_crc)
EOF

  $pl->d('SQL:', $sql);

  my $diffs = $csum_dbh->selectall_arrayref($sql, { Slice => {} } );
  $pl->d('TABLE DIFFS:', Dumper($diffs));

  # Shamelessly borrowed from mk-table-checksum.
  my @headers = qw(host db tbl chunk cnt_diff crc_diff boundaries);
  my $max_host = max(10, map { length($_->{host}) } @$diffs);
  my $max_db   = max(5, map { length($_->{db})  } @$diffs);
  my $max_tbl  = max(5, map { length($_->{tbl}) } @$diffs);
  my $fmt      = "%-${max_host}s %-${max_db}s %-${max_tbl}s %5s %8s %8s   %s";
  $pl->m(sprintf($fmt, map { uc } @headers));
  foreach my $tbl ( @$diffs) {
    $pl->m(sprintf($fmt, @{$tbl}{@headers}));
  }

  $csum_dbh->disconnect;
  $pl->end;
  return 0;
}

sub run_mk_checksum {
  my $cm = shift;
  my $master_dbh = undef;
  $pl->d("CHECKSUM ARGUMENTS:", Dumper(\@_));
  my $r = $pl->x(\&mk_table_checksum::main, @_);
  {
    my $fh = $r->{fh};
    local $INPUT_RECORD_SEPARATOR;
    $pl->d(<$fh>);
  }
  if($r->{rcode}) {
    $pl->ed("Error calling mk-table-checksum:", 'exit:', $r->{rcode}, $r->{error});
  }

  eval {
    my $ms = MasterSlave->new();
    my $master_dsn = $dp->parse("h=$cm,u=$user,p=$password");
    $master_dbh = $dp->get_dbh($dp->get_cxn_params($master_dsn));
    $pl->i("RETRIEVING CHECKSUMS FROM:", $cm);
    $ms->recurse_to_slaves(
      {
        dbh  => $master_dbh,
        dsn  => $master_dsn,
        dsn_parser => $dp,
        callback => \&save_to_central_server
      }
    );
    $master_dbh->disconnect();
    $csum_dbh->commit();
    $csum_dbh->{AutoCommit} = 0;
  };
  if($@) {
    $csum_dbh->rollback();
    $master_dbh->disconnect();
    $pl->ed("Error during retrieval of checksum results: $@");
  }
}

sub save_to_central_server {
  my ( $dsn, $dbh, $level, $parent ) = @_;
  my $central_table = "`$csum_dsn->{D}`.`$csum_dsn->{t}`";
  my $host = $dbh->quote($dsn->{h});
  my $del_sql = qq#DELETE FROM $central_table WHERE host=$host AND ts < NOW() - INTERVAL 12 HOUR#;
  $pl->d('SQL:', $del_sql);
  RETRY:
  eval {
    $csum_dbh->do($del_sql);
  };
  if($EVAL_ERROR) {
    die $EVAL_ERROR if($EVAL_ERROR !~ /Table '$csum_dsn->{D}.$csum_dsn->{t}' doesn't exist/);
    my $creat_sql =<<"    EOF";

      CREATE TABLE $central_table (
        host       char(100)    NOT NULL,
        db         char(64)     NOT NULL,
        tbl        char(64)     NOT NULL,
        chunk      int          NOT NULL,
        boundaries char(100)    NOT NULL,
        this_crc   char(40)     NOT NULL,
        this_cnt   int          NOT NULL,
        master_crc char(40)         NULL,
        master_cnt int              NULL,
        ts         timestamp    NOT NULL,
        PRIMARY KEY (host, db, tbl, chunk, ts),
        KEY `host_and_ts` (`host`,`ts`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    EOF
    $pl->d('SQL:', $creat_sql);
    $csum_dbh->do($creat_sql);
    goto RETRY;
  }

  unless($allow_slave_lag) {
    my $ms = MasterSlave->new();
    my $secs = $ms->get_slave_status($dbh)->{seconds_behind_master};
    if(!defined $secs) {
      $pl->e('slave', $dsn->{h}, 'has broken repl!');
      $pl->m('Will sleep until fixed. Safe to kill tool with Ctrl-C.');
    }
    elsif($secs > 0) {
      $pl->m('slave', $dsn->{h}, 'is not caught up with master. Lag:', $secs);
      $pl->m('Will sleep until fixed. Safe to kill tool with Ctrl-C.');
    }
    while(!defined $secs or $secs > 0) {
      $pl->d('slave', $dsn->{h}, 'is not caught up with master. Lag:', $secs);
      sleep(1);
      $secs = $ms->get_slave_status($dbh)->{seconds_behind_master};
    }
  }

  foreach my $r ( @{$dbh->selectall_arrayref('SELECT * FROM '. $repl_table, { Slice => {} })} ) {
    my $magic_cols = join(',', map { $_ } sort keys %$r);
    my $magic_vals = join(',', map { $dbh->quote($r->{$_}) } sort keys %$r);
    my $ins_sql = qq#INSERT INTO $central_table (host,$magic_cols) VALUES (${host},$magic_vals) #;
    $pl->d('SQL:', $ins_sql);
    $csum_dbh->do($ins_sql);
  }
  return;
}

exit(main(@ARGV)) unless(caller);

=pod

=head1 NAME

pdb-dsn-checksum - Checksum servers using PalominoDB DSN as source

=head1 SYNOPSIS

pdb-dsn-checksum -d <uri> -s <dsn> -R <db.table> -u <user> -p <pw>

All options mentioned above are required for operation.

=head1 EXAMPLES

  pdb-dsn-checksum --dsn /tmp/dsn.yml \
    --store h=ops,u=ops,p=ops,D=checksums,t=checksums \
    --replicate-table checksum.checksum --user csum \
    --password csum --pretend


=head1 OPTIONS

=over 8

=item --help,-?

This help.

=item --pretend

Don't do actions reported.

=item --only-report

Do not checksum, just bring central checksum table up to date with what are in the distributed tables.

=item --mk-table-checksum-path

Default: /usr/bin/mk-table-checksum.

Path to mk-table-checksum. Required to function as this tool is merely a wrapper around it.

=item --dsn,-d <uri>

PalominoDB YAML DSN.

=item --store,-s <dsn>

Maatkit-style DSN pointing to table (central checksum result storage).
Example:

  h=testdb1,u=root,p=pass,D=test_db,t=checksum_table

At present, this table is created automatically for you, and,
results older than 1 week are purged when updating it.
There is not currently a way to configure that.

=item --replicate-table,-R <db.tbl>

What database.table to use to store checksum results on servers.

The table needn't exist beforehand, mk-table-checksum will create it if it's missing.
The user and password given with L<"--user"> and L<"--password"> must be capable of creating, and replacing
into the table.

=item --cluster <cluster name>

Select a single cluster out of many enabled for checksumming in a dsn.

Normally, the behavior of this program is to checksum each cluster
listed as enabled in serial. This option negates that.

=item --chunk-size

Override the default chunk size of 50k rows, or, whatever is set
in the DSN.

=item --since

B<THIS DOES NOT DO WHAT YOU WANT. REALLY.>

Only checksum rows since some MySQL expression. See the
mk-table-checksum docs as this value is passed verbatim
onto that tool.

mk-table-checksum does some kind of wacky thing where it assumes
that if the expression given here evaluates to a timestamp,
then, the WHOLE TABLE should be checksumed if it has data
newer than that value. If it's not a timestamp, (i.e., a number),
then it'll checksum rows with values larger than that number.
Dumb, right?

What you really want is to setup the YAML DSN so that it contains
the appropriate where clauses. See below for how to do that.

=item --user,-u <user>

Username for all servers.

=item --password,-p <pw>

Password for user.

=item --logfile,-L <path>

Where to log debugging and informational messages.
Like all other PDB tools, you can also specify syslog:<facility>
to log to syslog.

Default: ./pdb-dsn-checksum.log

=item --lock <file>

Use file as a lockfile to prevent multiple concurrent runs.

=item --lock-timeout <seconds>

Only wait up to seconds for the lock to become available.

=item --allow-slave-lag,-a

Normally pdb-dsn-checksum will spin on each slave until it's caught
up with the master (i.e., 0 seconds behind), this is to ensure that
all checksums have been executed on the slave. This behavior will
make pdb-dsn-checksum spin indefinitely on a slave that is delayed
on purpose.

This flag disables the above behavior. If you have delayed slaves,
you must still make sure that they've executed all the checksum queries
before trusting any results from this tool.


=back

=head1 YAML DSN KEYS

This tool recognizes the following keys in a YAML DSN:
These all go on primary hosts, not clusters.

=over 4

=item checksum

type: boolean (y/n)

If 'n', then this tool will skip this cluster.

=item checksum_options:

type: group

Holds all options for the checksum tool.

Keys:

=over 4

=item tables

type: array

A list/array of tables (qualified with database name) to checksum.
Each item should be the key to a one element hash where the value
is the where clause to apply to that table.

Example:

  checksum_options:
    tables:
    - test.test_data: "ts > NOW() - INTERVAL 1 HOUR"

=item since

type: expression

This is the same as the L<--since> flag. Most likely you don't want this.

=item ignore_tables

type: array

Tables (qualified with database name) to ignore when checksumming, if L<tables>
wasn't specified.

=item ignore_databases

Databases to ignore (skip) when checksumming.

=item chunk_size

How large the chunks should be for checksumming.
Smaller chunks generally mean a slightly longer wait for results, but
less lock contention.

Default: 50,000

=back

=back


=cut

1;
