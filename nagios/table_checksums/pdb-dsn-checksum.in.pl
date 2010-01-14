#!/usr/bin/env perl
use strict;
use warnings FATAL => 'all';

# ##########################################################################
# ProcessLog package GIT_VERSION
# ##########################################################################
# ##########################################################################
# End ProcessLog package
# ##########################################################################

# ##########################################################################
# Pdb::DSN package GIT_VERSION
# ##########################################################################
# ##########################################################################
# End Pdb::DSN package
# ##########################################################################

package pdb_dsn_checksum;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Data::Dumper;
use Getopt::Long;
use Pod::Usage;
use List::Util qw(max);

use Pdb::DSN;
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

my @ignore_tables = ('mysql.slow_log', 'mysql.general_log');
my @ignore_databases = ();
my $mk_table_checksum_path = '/usr/bin/mk-table-checksum';

my $csum_dsn = undef;
my $csum_dbh = undef;

sub main {
  my (@ARGV) = @_;
  GetOptions(
    'help|?'  => sub { pod2usage(-verbose => 99); },
    'dsn|d=s' => \$dsnuri,
    'user|u=s' => \$user,
    'log=s'    => \$loguri,
    'password|p=s' => \$password,
    'store|S=s' => \$central_dsn,
    'replicate-table|R=s' => \$repl_table,
    'only-report' => \$only_report,
    'pretend' => \$pretend,
    'mk-table-checksum-path=s' => \$mk_table_checksum_path
  );

  unless(defined($central_dsn) and defined($dsnuri) and defined($user) and defined($password) and defined($repl_table)) {
    pod2usage(-message => '--dsn, --user, --password, --store, and --replicate-table are all required.', -verbose => 99);
    return 1;
  }

  $pl = ProcessLog->new($0, $loguri, undef);
  $pl->start;

  eval "require '$mk_table_checksum_path'";
  if($EVAL_ERROR) {
    $pl->e($EVAL_ERROR);
    return 1;
  }
  no warnings;
  *mk_table_checksum::print_inconsistent_tbls = \&main::save_to_central_server;
  use warnings FATAL => 'all';


  my $dsn = Pdb::DSN->new($dsnuri);
  my $dp = DSNParser->new({key => 't', 'desc' => 'Table to write to', copy => 0});
  $csum_dsn = $dp->parse($central_dsn);
  $csum_dbh = $dp->get_dbh($dp->get_cxn_params($csum_dsn));

  foreach my $c ($dsn->get_all_clusters()) {
    $pl->d("CLUSTER:", $c);
    my $s = $dsn->cluster_primary($c);
    $pl->i("CLUSTER ($c) PRIMARY:", $s);
    push(@checksum_masters, $s) if($dsn->server_checksum($s));
    if(defined(my $opts = $dsn->server_checksum_options($s))) {
      $pl->i("CLUSTER ($c) CHECKSUM OPTIONS:", Dumper($opts));
      if($opts->{ignore_tables}) {
        push @ignore_tables, @{$opts->{ignore_tables}};
      }
      if($opts->{ignore_databases}) {
        push @ignore_databases, @{$opts->{ignore_databases}};
      }
    }
  }

  unless ($only_report) {
    foreach my $cm (@checksum_masters) {
      $pl->i("CHECKSUMMING CLUSTER PRIMARY:",$cm);
      my @mk_args = ('--empty-replicate-table', '--replicate', $repl_table, '--user', $user, '--password', $password, '--ignore-databases', join(',', @ignore_databases), '--ignore-tables', join(',',@ignore_tables), $cm);
      run_mk_checksum(@mk_args);
    }
  }

  foreach my $cm (@checksum_masters) {
    $pl->i("RETRIEVING CLUSTER CHECKSUMS:", $cm);
    my @mk_args = ('--replicate-check', 2, '--replicate', $repl_table, '--user', $user, '--password', $password, $cm);
    run_mk_checksum(@mk_args);
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
  $pl->d("CHECKSUM ARGUMENTS:", Dumper(\@_));
  my $r = $pl->x(\&mk_table_checksum::main, @_);
  {
    my $fh = $r->{fh};
    local $INPUT_RECORD_SEPARATOR;
    $pl->d(<$fh>);
  }
  if($r->{error}) {
    $pl->es("Error calling mk-table-checksum:", $r->{error});
  }
}

sub save_to_central_server {
  my (%args) = @_;
  foreach my $arg ( qw(o dp dsn tbls) ) {
    die "I need $arg" unless $args{$arg};
  }
  my $central_table = "`$csum_dsn->{D}`.`$csum_dsn->{t}`";
  my $dbh = $args{dbh};
  my $dsn = $args{dsn};
  my $o   = $args{o};
  my $host = $dbh->quote($dsn->{h});
  $csum_dbh->do(
    qq# DELETE FROM $central_table WHERE host=$host #
  );
  foreach my $r ( @{$dbh->selectall_arrayref('SELECT * FROM '. $args{o}->get('replicate'), { Slice => {} })} ) {
    my $magic_cols = join(',', map { $_ } sort keys %$r);
    my $magic_vals = join(',', map { $dbh->quote($r->{$_}) } sort keys %$r);
    $csum_dbh->do(
      qq# INSERT INTO $central_table (host,$magic_cols) VALUES
      (${host},$magic_vals) #
    );
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

PalominoDB DSN URI.

=item --store,-s <dsn>

Maatkit-style DSN pointing to table (central checksum result storage).
Example:

  h=testdb1,u=root,p=pass,D=test_db,t=checksum_table

=item --replicate-table,-R <db.tbl>

What database.table to use to store checksum results on servers.

The table needn't exist beforehand, mk-table-checksum will create it if it's missing.
The user and password given with L<"--user"> and L<"--password"> must be capable of creating, and replacing
into the table.

=item --user,-u <user>

Username for all servers.

=item --password,-p <pw>

Password for user.

=head1 EXAMPLES

=cut

1;
