#!/bin/bash
PROG_NAME=$(basename $0)
profiler_config=$1
shift
slow_logs="$@"

mysql_host=""
mysql_schema="ops_prof"
mysql_username="sqlprofiler"
mysql_password="sqlprofiler"
mk_query_digest=$(which mk-query-digest || echo "not found.")
ttt_server_name=""

filter_file=""

usage() {
  echo "Usage: $0 <path to configuration> <slow log>..."
  echo "Git version: SCRIPT_GIT_VERSION"
  echo "The config file is just a set of shell variables."
  echo "Config variables:"
  echo "mysql_host       - mysql host to store review info on."
  echo "mysql_schema     - mysql database to store review info in. Default: $mysql_schema"
  echo "mysql_username   - mysql user. Default: $mysql_username"
  echo "mysql_password   - mysql password. Default: $mysql_password"
  echo "mk_query_digest  - Path to mk-query-digest (if not in path). Default: $mk_query_digest"
  echo "ttt_server_name  - Name of this machine as known by TTT."
  echo "                   Default: \`hostname -f\` (`hostname -f`)"
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

  # Do Not change the table names in the below command.
  # TTT-GUI presently requires these names. k?
  $mk_query_digest --create-review-table --create-review-history-table --no-report --review h=$mysql_host,u=$mysql_username,p=$mysql_password,D=$mysql_schema,t=sql_profiler_reviews --review-history D=$mysql_schema,t=sql_profiler_histories --filter "$filter" $slow_logs
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
