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

if [ -z $1 ];then
  echo "Error: usage $0 <port> <backup_dir>"
  echo "       ie: $0 3320 /tmp/backup     "
  exit 1
else
  if [ -z $2 ];then
    echo "Error: usage $0 <port> <backup_dir>"
    echo "       ie: $0 3320 /tmp/backup     "
    exit 1
  else
    port=$1
    backup_dir="${2}/${1}"
  fi
fi

logfile=${backup_dir}/dump.log

mkdir -p ${backup_dir} 2> /dev/null

exec > >(tee $logfile) 2>&1

echo "Start date: `date`"
date +"%s"
  chown -R mysql:mysql ${backup_dir}

db_list=`mysql -sNu root --socket=/data/mysql/m${port}/logs/mysql.sock -e "select distinct table_schema from information_schema.tables where table_schema not in ('information_schema')"`

for db in $db_list
do

  mkdir -p ${backup_dir}/${db} 2> /dev/null
  chown -R mysql:mysql ${backup_dir}
  # Need to add and test --master-data=2 --single-transaction . Not sure how it will work
  # since we're running it per schema.
  mysqldump -h localhost -u root --socket=/data/mysql/m${port}/logs/mysql.sock \
            --default-character-set=latin1 -q -Q \
            --tab=${backup_dir}/${db} \
            ${db}

# --no-data \
done

echo "End date: `date`"
date +"%s"

