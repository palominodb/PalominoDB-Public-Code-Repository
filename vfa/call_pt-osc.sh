#!/bin/bash
# author: dturner@palominodb.com
# 
# repo : wget --no-check-certificate https://raw.github.com/dturner-palominodb/dba/master/call_pt-osc.sh
# Must be run locally
# Notes:
#      mkdir 20120101_call_pt-osc
#  
#      Create files for each table changed.
#      20120101_call_pt-osc/alter01.sql
#      20120101_call_pt-osc/alter02.sql
#      Within each file the first line is the table and second on are the 
#      ddl statements.
#
#      IE:
#         entries
#         modify `current_tags` varchar(2047) DEFAULT NULL,
#         modify `position` int(11) DEFAULT NULL
#
#      Create .call_pt-osc with schema_filter=theschema
#
#
#       The script can be told to sleep with a sleep file.
#       IE: touch /tmp/call_pt-osc.sleep
#       And worst case use /tmp/call_pt-osc.stop to tell
#       the script to exit.
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

if [ -z $1 ];then
  echo "Error: usage call_pt-osc.sh [modification_date] [port]"
  exit 1
else
  # This is used to ensure previous modifications are not run.
  modification_date=$1
  modification_dir=${modification_date}_call_pt-osc
  if [ ! -d ${modification_dir} ];then
    echo "Creating ${modification_dir} for the alter.sql files"
    mkdir ${modification_dir} 2> /dev/null
    exit 0
  fi
fi

if [ -z $2 ];then
  port=3306
else
  port=$1
fi

source /usr/local/palominodb/scripts/vfa_lib.sh ${port}

sleep_file=/tmp/call_pt-osc.sleep
stop_file=/tmp/call_pt-osc.stop

if [ -e .call_pt-osc ];then
  source .call_pt-osc
else
  schema_filter="theschema"
fi

schema_list=`mysql ${socket} -sNe 'show databases' |grep ${schema_filter}`

# Remove these when running on some hosts
socket="--socket=$(get_socket ${port})"
dsn_socket=",S=$(get_socket ${port})"
echo "dsn_socket=${dsn_socket}"


logfile="/tmp/call_pt-osc.log"

# exec > >(tee $logfile) 2>&1

for file in $(ls ${modification_dir}/alter*.sql)
# for file in $(ls call_pt-osc.file*.${modification_date})
do
  
  table=`head -1 ${file}`
  stmt=`tail -n +2 ${file}`  
  echo "table=$table"
  # if no schema_list check the files for the schemas
  if [ -z "${schema_list}" ];then
     if [[ $table =~ "." ]];then
       schema_list=`echo $table |cut -d. -f1`
       table=`echo $table |cut -d. -f2`
     else 
       echo "Error: no schema specified for changes."
       exit 1
     fi
  else
    echo "${schema_list}"
    echo "${schema_list}" | wc -l
  fi
    
  for schema in ${schema_list}
  do
    if [ -e ${stop_file} ];then
      echo "Stop file ${stop_file} found. Exiting."
      exit 0
    fi

    while [ -e ${sleep_file} ]
    do
      echo "Sleep file ${sleep_file} found."
      echo "Sleeping 60s."
      sleep 60
    done
    
    echo "schema=$schema"
    echo "time pt-online-schema-change -u root --critical-load=Threads_Running=150 --alter \"${stmt}\" --execute D=${schema},t=${table}${dsn_socket}"
    time pt-online-schema-change -u root --critical-load=Threads_Running=150 --alter "${stmt}" --execute D=${schema},t=${table}${dsn_socket}
    # break
  done

done 2>&1 | tee $logfile

if [ `grep -i error ${logfile} | wc -l` -gt 0 ];then
  echo 
  echo "========================="
  echo "Error found in ${logfile}"
  grep -i error ${logfile}
  echo "========================="
else
  echo 
  echo "========================="
  echo "Completed without Error."
  echo "========================="
fi
