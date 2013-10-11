#!/bin/bash
# Purpose:
# Perform an in place defrag of all tables by
# using alter table statement

# Features to add
# o Check that there is space for the largest table, if not exit.
# o Fix the parm passing below
# o stop and start replication.

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

# Set port to 3306 if $1 isn't passed.
port=$1
port=${port:=3306}
# Example ./pdb-defrag.sh  3307 test


if [ -z $2 ];then
  schema_condition=""
else
  schema_condition="and table_schema in (\"$2\")"
fi

# Debug mode just gathers stats and doesn't actually perform
# the alters.
# debug=1


# The minimum number MB that must remain during a reorg.
minimum_mb_free_space=10000

# The minimum amount of fragmentation to perform a reorg.
# Set to 0 to reorg all tables.
minimum_mb_fragmentation=0

# The minimum table size to reorg.
minimum_mb_table=20000

# The maximum size of table to reorg. Any large than
# N MB should be reorged with percona's online schema change
# script
maximum_mb_table=2000000

source /usr/local/palominodb/scripts/vfa_lib.sh ''

log_dir="/var/log/pdb-defrag"
mkdir ${log_dir} 2> /dev/null

# start with 1 slave, for 3 or 4 days
# defrag everything, show cost savings by table,
# start with one shard
# then do all.
# we will revisit tables that Moss works on

# Functions
function get_fragmentation_info() {

  port=$1
  file=${log_dir}/pdb-defrag-tables-${port}-${run_date}.dat

  stmt="
  select
    concat(table_schema,'.', table_name,':',
    round(data_length/1024),  ':',
    round(index_length/1024), ':',
    round(data_free/1024),    ':',
    # current_data_file_size
    round((data_length + index_length + data_free)/1024), ':',
    # defragged_data_file_size
    round((data_length + index_length)/1024)
    )
  from
    information_schema.tables
  where
    table_schema not in ('information_schema','mysql','performance_schema')
  and
    engine='innodb' ${schema_condition}
  and
    (data_free) / 1024 / 1024 >= ${minimum_mb_fragmentation}
  and
    (data_length + index_length) / 1024 / 1024 >= ${minimum_mb_table}
  and
    (data_length + index_length) / 1024 / 1024 <= ${maximum_mb_table}
  order by
    data_length + index_length asc;
  "
  echo "# table : data : indx : free : current file size : expected file size" > ${file}
  mysql --socket=$(get_socket $port) -sNe "$stmt" >> ${file}

}


function get_total_fragmentation_info() {

  port=$1
  file=${log_dir}/pdb-defrag-total-${port}-${run_date}.dat

  stmt="
  select
    concat(
    round(sum(data_length)/1024),  ':',
    round(sum(index_length)/1024), ':',
    round(sum(data_free)/1024),    ':',
    # current_data_file_size
    round(sum(data_length + index_length + data_free)/1024), ':',
    # defragged_data_file_size
    round(sum(data_length + index_length)/1024)
    )
  from
    information_schema.tables
  where
    table_schema not in ('information_schema','mysql','performance_schema')
  and
    engine='innodb' ${schema_condition}
  and
    (data_free) / 1024 / 1024 >= ${minimum_mb_fragmentation}
  and
    (data_length + index_length) / 1024 / 1024 >= ${minimum_mb_table}
  and
    (data_length + index_length) / 1024 / 1024 <= ${maximum_mb_table}
  "
  echo "#data : indx : free : current file size : expected file size" > ${file}
  mysql --socket=$(get_socket $port) -sNe "$stmt" >> ${file}

}

function get_disk_stats() {
  port=$1
  datadir=`show_datadir $port`
  file=${log_dir}/pdb-defrag-datadir-${port}-${run_date}.dat

  df_result=`df -P -k ${datadir}|tail -1 |awk '{print $2 ":" $3 ":" $4}'`
  du_result=`du -P -ks ${datadir}|tail -1 | awk '{print $1}'`


  echo "# total : used : avail : mysql usage" > ${file}
  echo "${df_result}:${du_result}" >> ${file}

}

function get_table_list() {
  port=$1

  stmt="
  select
    concat(table_schema,'.', table_name,':',
    round(data_length/1024),  ':',
    round(index_length/1024), ':',
    round(data_free/1024),    ':',
    # current_data_file_size
    round((data_length + index_length + data_free)/1024), ':',
    # defragged_data_file_size
    round((data_length + index_length)/1024), ':',
    engine
    )
  from
    information_schema.tables
  where
    table_schema not in ('information_schema','mysql','performance_schema')
  and
    engine='innodb' ${schema_condition}
  and
    (data_free) / 1024 / 1024 >= ${minimum_mb_fragmentation}
  and
    (data_length + index_length) / 1024 / 1024 >= ${minimum_mb_table}
  and
    (data_length + index_length) / 1024 / 1024 <= ${maximum_mb_table}
  order by
    data_length + index_length asc;
  "
  mysql --socket=$(get_socket $port) -sNe "$stmt"

}

function stop_rep() {
  port=$1
  stmt='stop slave'
  mysql --socket=$(get_socket $port) -sNe "$stmt"

}

function start_rep() {
  port=$1
  stmt='start slave'
  mysql --socket=$(get_socket $port) -sNe "$stmt"

}

function check_runlog_for_table() {
  port=$1
  table=$2
  runlog=/tmp/pdb-defrag-run-${port}.log

  if [ -e ${runlog} ];then
    grep -iw ${table} $runlog 2> /dev/null|wc -l
  else
     echo 0
  fi

}

function call_alter_tables() {
  port=$1
  file=${log_dir}/pdb-defrag-alters-${port}-${run_date}.log
  > ${file}
  echo "tail -f ${log_dir}/pdb-defrag-alters-${port}-${run_date}.log"
  echo "to see progress."

# DEBUG
#  echo "Stopping replication" >> ${file}
#  stop_rep ${port}

  for table_info in ${table_list}
  do
    table=`echo ${table_info} | cut -d: -f1`
   
    # Check the runlog to see if the table was already reorged.
    if [ $(check_runlog_for_table ${port} ${table}) -gt 0 ];then
      echo "Skipping ${table}."
      continue
    fi

    engine=`echo ${table_info} | cut -d: -f7`
    stmt="set sql_log_bin=0;alter table ${table} engine=${engine}"
    echo "${stmt}" >> ${file}
    if [ -z "$debug" ];then
      mysql -vvv --socket=$(get_socket $port) -sNe "$stmt" >> ${file}
    fi

  done

# DEBUG
#   echo "Starting replication" >> ${file}
#   start_rep ${port}
}

# Get the current date for gathering stats befor the reorg.
run_date=$(date +"%Y%m%d%H%M%S")

get_disk_stats ${port}
get_fragmentation_info ${port}
get_total_fragmentation_info ${port}

table_list=`get_table_list ${port}`
call_alter_tables ${port}

# Get the current date for gathering stats after the reorg.
run_date=$(date +"%Y%m%d%H%M%S")

get_disk_stats ${port}
get_fragmentation_info ${port}
get_total_fragmentation_info ${port}


#   bytes_avail_datadir=`df -P -k ${datadir} |tail -1 |awk '{print $4 "* 1024"}' |bc`
#   echo bytes_avail_datadir=$bytes_avail_datadir
