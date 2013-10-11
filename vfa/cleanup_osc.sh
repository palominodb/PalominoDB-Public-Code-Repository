#!/bin/bash
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

port=$1
port=${port:=3306}

out_file=/tmp/cleanup_osc_${port}.sql
echo "set sql_log_bin=0;" > ${out_file}

stmt="select concat(table_schema,'.', table_name) from information_schema.tables where table_name like '__osc%'"


table_list=`mysql --socket=$(get_socket $port) -sNe "$stmt"`

for table in ${table_list}
do
  echo "drop table ${table};" >> ${out_file}

done

cat ${out_file}

echo "mysql -vvv < ${out_file}"
