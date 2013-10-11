#!/bin/bash

# Purpose:
# Perform online schema change using pt-osc on all tables
# that aren't referenced by a foreign key. 

# Other:
#       You can pause the script by using 
#       a sleep file. IE: touch /tmp/pdb-mass-pt-osc-3306.sleep
#
#       You can cleanly stop the script by using 
#       a stop file. IE: touch /tmp/pdb-mass-pt-osc-3306.stop
#


# Features to add
# o Check each time a table should be reorge that ther is enough space 
#   on the filesystem
# o Fix the parm passing below
# o stop and start replication.

# Links
# FB's osc files
# http://bazaar.launchpad.net/~mysqlatfacebook/mysqlatfacebook/tools/files/head:/osc/
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
prefix="pdb-mass-pt-osc"

if [ -z $2 ];then
  schema_condition=""
else
  schema_condition="and table_schema in (\"$2\")"
fi

# Debug mode just gathers stats and doesn't actually perform
# the alters.
# debug=1


# The minimum amount of fragmentation to perform a reorg.
# Set to 0 to reorg all tables.
minimum_mb_fragmentation=0

# The minimum size of table to reorg. Added since we're
# having to do these in batches.
minimum_mb_table=0

# The maximum size of table to reorg. Any large than
# N MB should be reorged with percona's online schema change
# script
maximum_mb_table=1000000

source /usr/local/palominodb/scripts/vfa_lib.sh ''

log_dir="/var/log/${prefix}"
mkdir ${log_dir} 2> /dev/null

# start with 1 slave, for 3 or 4 days
# defrag everything, show cost savings by table,
# start with one shard
# then do all.
# we will revisit tables that Moss works on

# Functions
function get_fragmentation_info() {

  port=$1
  file=${log_dir}/${prefix}-tables-${port}-${run_date}.dat

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
    table_schema like 'zd_shard%'
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
  file=${log_dir}/${prefix}-total-${port}-${run_date}.dat

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
    table_schema like 'zd_shard%'
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
  file=${log_dir}/${prefix}-datadir-${port}-${run_date}.dat

  df_result=`df -P -k ${datadir}|tail -1 |awk '{print $2 ":" $3 ":" $4}'`
  du_result=`du -P -ks ${datadir}|tail -1 | awk '{print $1}'`


  echo "# total : used : avail : mysql usage" > ${file}
  echo "${df_result}:${du_result}" >> ${file}

}

function get_table_list() {
  port=$1

  stmt="

  SELECT /*!99999 pdb: pdb-mass-pt-osc.sh  */
      concat(i.table_schema,'.', i.table_name,':',
      round(i.data_length/1024),  ':',
      round(i.index_length/1024), ':',
      round(i.data_free/1024),    ':',
      # current_data_file_size
      round((i.data_length + i.index_length + i.data_free)/1024), ':',
      # defragged_data_file_size
      round((i.data_length + i.index_length)/1024), ':',
      engine
      )
  FROM
  information_schema.tables AS i
  LEFT JOIN
    (
    SELECT table_schema, table_name
    FROM
      information_schema.key_column_usage
    WHERE
      referenced_table_name IS NOT NULL
    UNION
    SELECT referenced_table_schema as table_schema, referenced_table_name as table_name
    FROM
      information_schema.key_column_usage
    WHERE
      referenced_table_name IS NOT NULL
    ) AS fk
  ON (i.table_schema=fk.table_schema)
     AND
     (i.table_name=fk.table_name)
  WHERE
    i.table_schema not in ('information_schema','mysql','performance_schema')
  AND 
    i.table_schema like 'zd_shard%'
  AND
    i.engine='innodb' ${schema_condition}
  AND
    (i.data_free) / 1024 / 1024 >= ${minimum_mb_fragmentation}
  and
    (data_length + index_length) / 1024 / 1024 >= ${minimum_mb_table}
  AND
    (i.data_length + i.index_length) / 1024 / 1024 <= ${maximum_mb_table}
  AND
  fk.table_schema IS NULL
  order by
    i.data_length + i.index_length asc;
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

# Rather than killing the script it's nice to cleanly stop
# the script
function check_for_stop_file() {
 
  sleep_file=/tmp/pdb-mass-pt-osc-${port}.sleep
  stop_file=/tmp/pdb-mass-pt-osc-${port}.stop
  sleep_time=6

  if [ -e ${stop_file} ];then
    echo "Stop file found. Exiting"
    exit 0
  fi

  while [ -e ${sleep_file} ]
  do 
    echo "Sleep file found. Sleeping ${sleep_time}s"
    sleep ${sleep_time}
  done
}

function call_alter_tables() {
  port=$1
  file=${log_dir}/${prefix}-alters-${port}-${run_date}.log
  sql_file=/tmp/pdb-mass-pt-osc.sql
  osc_file=/usr/local/palominodb/scripts/fb/osc_wrapper.php
  > ${file}
  > ${sql_file}
  echo "tail -f ${log_dir}/${prefix}-alters-${port}-${run_date}.log"
  echo "to see progress."

  for table_info in ${table_list}
  do
    check_for_stop_file

    database=`echo ${table_info} |cut -d: -f1 |cut -d. -f1`
    table=`echo ${table_info} | cut -d: -f1 | cut -d. -f2`
    engine=`echo ${table_info} | cut -d: -f7`

    stmt="alter table ${table} engine=${engine}"
    echo "${stmt}" >> ${sql_file}


    echo "pt-online-schema-change -u root --alter \" engine=${engine} \" --alter-foreign-keys-method=rebuild_constraints --execute D=${database},t=${table},S=$(get_socket $port)" >> ${file}
    if [ -z ${debug} ];then
      pt-online-schema-change -u root --alter " engine=${engine} " --alter-foreign-keys-method=rebuild_constraints --execute D=${database},t=${table},S=$(get_socket $port) >> ${file} 2>&1

    fi

  done

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
