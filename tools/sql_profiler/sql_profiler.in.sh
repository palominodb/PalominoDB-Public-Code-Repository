#!/bin/bash
# sql_profiler.in.sh
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
  echo "Git version: SCRIPT_GIT_VERSION"
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

## BEGIN note_db_host.filter GIT_VERSION ##
## END note_db_host.filter ##
FILTER_INCLUDE_EOF

main "$filter_file"
