#!/bin/bash
# author: dturner@palominodb.com
# purpose: create a file with all the tables and their
#          coresponding checksums. The file can be then
#          used to compare with another database using
#          standard utilities like diff.
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
  echo "Error: usage $0 <port>"
  exit 1
else
  port=$1
fi

source /usr/local/palominodb/scripts/vfa_lib.sh ""

hostname=`hostname`

cmd="select concat(table_schema,'.',table_name) \
     from information_schema.tables where table_schema \
     not in ('mysql','performance_schema','information_schema') \
     order by table_schema, table_name"

table_list=$(mysql --socket=$(get_socket ${port}) -sNe "$cmd")

for table in $table_list
do
 cmd="CHECKSUM TABLE ${table}"
 mysql --socket=$(get_socket ${port}) -sNe "$cmd"
done  2>&1 | tee pdb_checksum_to_file_${hostname}_${port}.txt


