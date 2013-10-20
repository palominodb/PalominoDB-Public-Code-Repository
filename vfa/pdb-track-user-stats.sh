#!/bin/bash
# author: dturner
# filename: pdb-track-table-usage.sh
# purpose: logs the total reads to each of the tables in all databases
#
# requires: set global userstat_running=1;
#
# features: should add a conf file with items like exlusion lists and something
#           the works with regexp so it excludes types of tables as well.
#
#   Copyright 2013 Palominodb
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

source /usr/local/palominodb/scripts/vfa_lib.sh ''

# The file used to track table usage.
data_file=/var/log/pdb-track-user-stats.dat
# Once this is reached trim file from beginning. This requires an additional
# 50% of the disk space for max_data_file_mb until the previous file has been
# removed.
max_data_file_mb=100

# main

port_list=$(show_ports)

for port in ${port_list}
do
  # The query that returns table usage for the current period.
  stmt="
  SELECT
    CONCAT_WS
    (',',
      date_format(now(),'%Y%m%d:%H%i%s'),
      \"$port\",
      USER,
      TOTAL_CONNECTIONS,
      CONCURRENT_CONNECTIONS,
      CONNECTED_TIME,
      BUSY_TIME,
      CPU_TIME,
      BYTES_RECEIVED,
      BYTES_SENT,
      BINLOG_BYTES_WRITTEN,
      ROWS_FETCHED,
      ROWS_UPDATED,
      TABLE_ROWS_READ,
      SELECT_COMMANDS,
      UPDATE_COMMANDS,
      OTHER_COMMANDS,
      COMMIT_TRANSACTIONS,
      ROLLBACK_TRANSACTIONS,
      DENIED_CONNECTIONS,
      LOST_CONNECTIONS,
      ACCESS_DENIED,
      EMPTY_QUERIES
    ) AS result
  FROM
    information_schema.user_statistics
  ORDER BY
    user
  "

  for result in `mysql --socket=$(get_socket ${port}) -sNe "$stmt"`
  do
    echo "$result" >> $data_file
  done


done
