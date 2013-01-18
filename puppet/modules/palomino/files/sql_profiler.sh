#!/bin/bash
# sql_profiler.sh - a wrapper for mk-query-digest to profile a database, store
# the results in a local table, and send a report email if desired.
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

PROG_NAME=$(basename $0)
profiler_config=$1
shift
slow_logs="$@"

mysql_host=""
mysql_socket=""
mysql_schema="ops_prof"
mysql_username="sqlprofiler"
mysql_password="sqlprofiler"
mk_query_digest=$(which mk-query-digest || echo "not found.")
ttt_server_name=""
email_to=""

filter_file=""

usage() {
  echo "Usage: $0 <path to configuration> <slow log>..."
  echo "Git version: 9624d7a375e81e544fa0eb367346fe3e93c834a7"
  echo "The config file is just a set of shell variables."
  echo "Config variables:"
  echo "mysql_host       - mysql host to store review info on."
  echo "mysql_socket     - mysql socket to use. Only used if mysql_host is localhost or empty."
  echo "mysql_schema     - mysql database to store review info in. Default: $mysql_schema"
  echo "mysql_username   - mysql user. Default: $mysql_username"
  echo "mysql_password   - mysql password. Default: $mysql_password"
  echo "mk_query_digest  - Path to mk-query-digest (if not in path). Default: $mk_query_digest"
  echo "ttt_server_name  - Name of this machine as known by TTT."
  echo "                   Default: \`hostname -f\` (`hostname -f`)"
  echo "email_to         - Where to send email. No default (not sent)."
  echo ""
}

cleanup() {
  rm -f "$filter_file"
}

main() {
  filter=$1
  if [[ -z "$profiler_config" ]]; then
    usage
    cleanup
    exit 1
  fi

  if [[ ! -f "$profiler_config" ]]; then
    usage
    cleanup
    echo "ERROR: Could not find configuration: '$profiler_config'"
    exit 1
  fi

  . $profiler_config

  # Push hostname into the filter via the environment.
  # But only if it's set.
  if [[ -n "$ttt_server_name" ]]; then
    export ttt_server_name
  fi

  socket=""
  if [[ -z "$mysql_host" || "$mysql_host" = "localhost" ]]; then
    socket="S=$mysql_socket,"
  fi

  # Do Not change the table names in the below commands.
  # TTT-GUI presently requires these names. k?

  ## Parse slow logs and load the review tables
  $mk_query_digest --create-review-table --create-review-history-table --no-report --review ${socket}h=$mysql_host,u=$mysql_username,p=$mysql_password,D=$mysql_schema,t=sql_profiler_queries --review-history D=$mysql_schema,t=sql_profiler_histories --limit 100% --filter "$filter" $slow_logs

  ## if we want email, parse AGAIN and generate the report
  ## This is necessary to get the top N queries. Fortunately,
  ## most slow logs are pretty small, so, this isn't a horrific task.
  if [[ -n $email_to ]]; then
    $mk_query_digest --review ${socket}h=$mysql_host,u=$mysql_username,p=$mysql_password,D=$mysql_schema,t=sql_profiler_queries --review-history D=$mysql_schema,t=sql_profiler_histories $slow_logs | mail -s "SQLProfiler report for $ttt_server_name" $email_to
  fi

  cleanup
}

filter_file=$(mktemp "$PROG_NAME.XXXXXX")
# The leading new-line before '## BEGIN..' is required so that the filter inserts
# neatly into mk-query-digest. You will get perl compile errors otherwise.
cat <<'FILTER_INCLUDE_EOF' >$filter_file

## BEGIN note_db_host.filter 84c9298a532f03a9c722b61650e0a6df9f6b5367 ##

use Sys::Hostname;
use constant MKFILTERDEBUG => $ENV{MKFILTERDEBUG};
my $act = sub {
  die("I require --review") if !$qv or !$qv_dbh;
  Transformers->import(qw(parse_timestamp));
  my $tried_create_table=0;
  my $evt = shift;
  my @tbl=@{$review_dsn}{qw(D)};
  $tbl[1] = "sql_profiler_hosts";
  my $tbl=$q->quote(@tbl);
  my $hostname = $ENV{ttt_server_name} ? $ENV{ttt_server_name} : hostname;
  my $sql =<<"  EOSQL";
    INSERT INTO $tbl
    (checksum, host, ip, type, first_seen, last_seen)
    VALUES(CONV(?, 16, 10), ?, INET_ATON(?), ?, COALESCE(?, NOW()), COALESCE(?, NOW()))
    ON DUPLICATE KEY UPDATE
      first_seen = IF(
        first_seen IS NULL,
        COALESCE(?, NOW()),
        LEAST(first_seen, COALESCE(?, NOW()))),
      last_seen = IF(
        last_seen IS NULL,
        COALESCE(?, NOW()),
        GREATEST(last_seen, COALESCE(?, NOW())))
  EOSQL
  MKFILTERDEBUG && _d("SQL to insert host csum mapping:", $sql);
  MKFILTERDEBUG && _d("Event structure:", Dumper($evt));
  # No easy way to prevent this being called many times.
  my $insert_sth = $qv_dbh->prepare_cached($sql);
REINSERT:
  eval {
    # Insert the machine we are parsing on as the destination.
    # No way to specify that this is not the case right now.
    # The destination is the database server that handled the query.
    # At some point, the IP should be filled in here too.
    $insert_sth->execute(
      make_checksum($evt->{fingerprint}),
      $hostname, 0, 'DESTINATION', map { parse_timestamp($evt->{ts}) } qw(first_seen last_seen first_seen first_seen last_seen last_seen));
    # Insert $evt->{ip} as the source.
    # The source is the address that issued the query.
    $insert_sth->execute(
      make_checksum($evt->{fingerprint}),
      $evt->{host}, $evt->{ip} ? $evt->{ip} : 0, 'SOURCE', map { parse_timestamp($evt->{ts}) } qw(first_seen last_seen first_seen first_seen last_seen last_seen));
    1;
  };
  if($@ and !$tried_create_table) {
    _d("Insert had error($@), trying create table.");
    # db_host limited to 60 chars since mysql won't store more
    # than that anyway.
    my $sql =<<"    EOCSQL";
      CREATE TABLE IF NOT EXISTS $tbl (
        checksum BIGINT UNSIGNED NOT NULL,
        host  VARCHAR(60) NOT NULL,
        ip    INTEGER UNSIGNED NOT NULL DEFAULT 0,
        type  ENUM('SOURCE', 'DESTINATION', 'UNKNOWN') NOT NULL DEFAULT 'UNKNOWN',
        first_seen DATETIME,
        last_seen  DATETIME,
        PRIMARY KEY(checksum, host),
        KEY(type)
      )
    EOCSQL
    MKFILTERDEBUG && _d("SQL for create table:", $sql);
    $tried_create_table = 1;
    eval { $qv_dbh->do($sql) };
    MKFILTERDEBUG && _d("CREATE TABLE Return:", $@);
    goto REINSERT
  }
  1;
};
$act->($event);
1
## END note_db_host.filter ##
FILTER_INCLUDE_EOF

main "$filter_file"
