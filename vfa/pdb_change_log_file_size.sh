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

source /usr/local/palominodb/scripts/vfa_lib.sh

datadir=`show_datadir`
logfile0="${datadir}/ib_logfile0"
logfile1="${datadir}/ib_logfile1"

/etc/init.d/mysqld stop

if [ $? -eq 0 ];then
  mysql_process_count=`pgrep mysqld|wc -l`
  if [ ${mysql_process_count} -gt 0 ];then
    pkill -9 mysqld
  fi
else
  "Error: problem stopping the database. Exiting."
  exit 1
fi

echo "Sleeping 5 until proven."

if [ -e ${logfile0} ];then
  echo "Found ${logfile0}"
  echo "mv ${logfile0} ${logfile0}.bak"
  mv ${logfile0} ${logfile0}.bak
  if [ $? -gt 0 ];then
    echo "Error: mv of ${logfile0} failed."
    exit 1
  else
    echo "mv of ${logfile0} to bak.${logfile0} succeeded."
  fi
else
  echo "Error: cannot find ${logfile0}"
fi

if [ -e ${logfile1} ];then
  echo "Found ${logfile1}"
  echo "mv ${logfile1} ${logfile1}.bak"
  mv ${logfile1} ${logfile1}.bak
  if [ $? -gt 0 ];then
    echo "Error: mv of ${logfile1} failed."
    exit 1
  else
    echo "mv of ${logfile1} to bak.${logfile1} succeeded."
  fi
else
  echo "Error: cannot find ${logfile1}"
fi


/etc/init.d/mysqld start

