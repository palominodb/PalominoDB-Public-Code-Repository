#!/bin/bash
# author: dturner@palominodb.com
# file: pdb-check-maxvalue.sh
# purpose: check for max values in all columns of integer types that have
#          reached N pct of the maximum value for that type of integer.
#
# repo: https://github.com/dturner-palominodb/dba
#
#       To download just this file do the following:
#       wget --no-check-certificate https://raw.github.com/dturner-palominodb/dba/master/pdb-check-maxvalue.sh
#
# phase II features:
#                   config file  : many options would be good to have as options in a config file
#
#                   store history: the values returned tell us a lot about the growth rate of certain columns.
#
#                   exlusion list: there will be some columns that clients don't care about.
#
#                   integrate with nagios: we need all clients informed when max values will be reached for their columns
#
# Notes: consider an option to exclude columns without indexes
#        Mark's suggested limiting to just pks. 
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



vfa_lib_file="/usr/local/palominodb/scripts/vfa_lib.sh"


lagcheck=1
begin=1
pk=""
while getopts ":s:p:b:i:klmh" opt ; do

  case $opt in
    s)
      pct_allowed=`echo $OPTARG | sed s/%//g`;;
    p)
      port=$OPTARG;;
    b)
      begin=$OPTARG;;
    i)
      inst=$OPTARG;;
    k)
      pk="and extra like 'auto_increment'";;
    l)
      lagcheck=0;;
    m)
      multi=1;; # we'll be launching multiple instances of this scipt, we need random filenames to do so
    h)
      echo "Usage:"
      echo "-s <pct_allowed> -p <port> -b <first statement - default 0> -k <if set, will check only pk - if not set, will check all - default> -l <if set, disable lag check>"
      exit 1;;
    *)
      echo "try -h"
      exit;;

  esac
done
if [ -z $pct_allowed ] ; then
  echo "Usage:"
  echo "-s <pct_allowed> -i <host:port> -p <port> -b <first statement - default 0> -k <if set, will check only pk - if not set, will check all - default>"
  exit 1;
fi

if [ ! -z ${inst} ];then
  if [[ $inst =~ :33[0-9][0-9] ]];then
    inst_host=`echo ${inst}|cut -d: -f1`
    inst_port=`echo ${inst}|cut -d: -f2`
  else
    if [ ! -z $port ];then
      inst_host=${inst}
      inst_port=${port}
    else
      inst_host=${inst}
      inst_port=3306
    fi
  fi
fi

#if [ -z $1 ];then
#  echo "Error: usage $0 <PCT_ALLOWED> <PORT> <FIRST_STATEMENT - default 0> <pk - optional, if you want to check only PK's - default, no>"
#  echo "       ie: $0 60 3307 10 (pk)             "
#  exit 1
#else
# pct_allowed=`echo $1 | sed s/%//g`
# port=$2
# begin=${3:-1} # 1 - because array starts from 0 and we want to exclude first line
# if [ -z $4 ] ; then pk="" ; else pk="and extra like 'auto_increment'" ; fi


#fi


if [ -e ${vfa_lib_file} ];then
  source ${vfa_lib_file} ''
  socket_info="--socket=$(get_socket ${port:=3306})"
  mysql_command="mysql ${socket_info}"
else
  if [ -z ${inst} ];then
    socket_info=""
    mysql_command="mysql ${socket_info}"
  else
    mysql_command="mysql -h $inst_host -P $inst_port"
  fi
fi


function gen_random_filename {
  rand=${RANDOM}
  sql_file="pdb-check-maxvalue${rand}.sql"
  proc_file="pdb-check-maxvalue${rand}.proc"
  if [ -e ${sql_file} ] ; then gen_random_filename ; fi
  if [ -e ${proc_file} ] ; then gen_random_filename ; fi

}

if (( multi == 1 )) ; then
  gen_random_filename
else
  sql_file="pdb-check-maxvalue.sql"
  # The generated statements
  proc_file="pdb-check-maxvalue.proc"
fi


cat > ${sql_file} <<EOF
select
  concat('select CONCAT_WS('':'',''',
  col.table_schema,'.',table_name,'.',column_name,
  ''', ',
  'round(ifnull(max(\`', column_name, '\`),0) / ',
  (CASE
      1
    WHEN
      replace(column_type,' zerofill','') regexp '^tinyint\\\([0-9]*\\\)$'          THEN ~0 >> 57 #tiny   int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^tinyint\\\([0-9]*\\\) unsigned$' THEN ~0 >> 56 #tiny   int unsigned
    WHEN
      replace(column_type,' zerofill','') regexp '^smallint\\\([0-9]*\\\)$'          THEN ~0 >> 49 #small  int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^smallint\\\([0-9]*\\\) unsigned$' THEN ~0 >> 48 #small  int unsigned
    WHEN
      replace(column_type,' zerofill','') regexp '^mediumint\\\([0-9]*\\\)$'          THEN ~0 >> 41 #medium int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^mediumint\\\([0-9]*\\\) unsigned$' THEN ~0 >> 40 #medium int unsigned
    WHEN
      replace(column_type,' zerofill','') regexp '^int\\\([0-9]*\\\)$'          THEN ~0 >> 33 #       int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^int\\\([0-9]*\\\) unsigned$' THEN ~0 >> 32 #       int unsigned
    WHEN
      replace(column_type,' zerofill','') regexp '^bigint\\\([0-9]*\\\)$'          THEN ~0 >>  1 #big    int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^bigint\\\([0-9]*\\\) unsigned$' THEN ~0       #big    int unsigned
    ELSE
      'failed'
  END),
  ' * 100 )',
  ', round(ifnull(max(\`', column_name, '\`),0)),',
  (CASE
      1
    WHEN
      replace(column_type,' zerofill','') regexp '^tinyint\\\([0-9]*\\\)$'          THEN ~0 >> 57 #tiny   int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^tinyint\\\([0-9]*\\\) unsigned$' THEN ~0 >> 56 #tiny   int unsigned
    WHEN
      replace(column_type,' zerofill','') regexp '^smallint\\\([0-9]*\\\)$'          THEN ~0 >> 49 #small  int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^smallint\\\([0-9]*\\\) unsigned$' THEN ~0 >> 48 #small  int unsigned
    WHEN
      replace(column_type,' zerofill','') regexp '^mediumint\\\([0-9]*\\\)$'          THEN ~0 >> 41 #medium int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^mediumint\\\([0-9]*\\\) unsigned$' THEN ~0 >> 40 #medium int unsigned
    WHEN
      replace(column_type,' zerofill','') regexp '^int\\\([0-9]*\\\)$'          THEN ~0 >> 33 #       int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^int\\\([0-9]*\\\) unsigned$' THEN ~0 >> 32 #       int unsigned
    WHEN
      replace(column_type,' zerofill','') regexp '^bigint\\\([0-9]*\\\)$'          THEN ~0 >>  1 #big    int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^bigint\\\([0-9]*\\\) unsigned$' THEN ~0       #big    int unsigned
    ELSE
      'failed'
  END),
  ') as INFO ',
  'from \`', col.table_schema, '\`.\`', table_name, '\` '
  'having round(ifnull(max(\`', column_name, '\`),0) / ',
  (CASE
      1
    WHEN
      replace(column_type,' zerofill','') regexp '^tinyint\\\([0-9]*\\\)$'          THEN ~0 >> 57 #tiny   int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^tinyint\\\([0-9]*\\\) unsigned$' THEN ~0 >> 56 #tiny   int unsigned
    WHEN
      replace(column_type,' zerofill','') regexp '^smallint\\\([0-9]*\\\)$'          THEN ~0 >> 49 #small  int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^smallint\\\([0-9]*\\\) unsigned$' THEN ~0 >> 48 #small  int unsigned
    WHEN
      replace(column_type,' zerofill','') regexp '^mediumint\\\([0-9]*\\\)$'          THEN ~0 >> 41 #medium int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^mediumint\\\([0-9]*\\\) unsigned$' THEN ~0 >> 40 #medium int unsigned
    WHEN
      replace(column_type,' zerofill','') regexp '^int\\\([0-9]*\\\)$'          THEN ~0 >> 33 #       int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^int\\\([0-9]*\\\) unsigned$' THEN ~0 >> 32 #       int unsigned
    WHEN
      replace(column_type,' zerofill','') regexp '^bigint\\\([0-9]*\\\)$'          THEN ~0 >>  1 #big    int signed
    WHEN
      replace(column_type,' zerofill','') regexp '^bigint\\\([0-9]*\\\) unsigned$' THEN ~0       #big    int unsigned
    ELSE
      'failed'
  END),
  ' * 100) > ${pct_allowed} ',
  ';') AS a
from
  information_schema.columns col
left join
  information_schema.views
using(TABLE_NAME)
where VIEW_DEFINITION is NULL
and
  # data_type in ('tinyint')
  data_type in ('tinyint','smallint','mediumint','int','integer','bigint')
${pk}
and
#  table_schema not in ('VALUE WILL BE AN OPTION IN FUTURE VERSION. HARD CODE IF NECESSARY');
 col.table_schema not in ('mysql','information_schema','VALUE WILL BE AN OPTION IN FUTURE VERSION. HARD CODE IF NECESSARY');


EOF



${mysql_command}  < ${sql_file} > ${proc_file}

# For debugging
# cat ${proc_file}


function check_sbm {

  sbm=`${mysql_command} -e "show slave status\G" | grep Seconds | awk '{print $2}'`

  if [ ! -n $sbm ] && [ ${sbm} -gt 10 ] ; then
    echo -en "Replication is lagging by ${sbm} seconds, waiting...\r"
    sleep 5;
    check_sbm
  fi

}


old_IFS=$IFS


IFS=$'\n'


line=($(cat ${proc_file}))


IFS=$old_IFS


size=${#line[@]}


counter=$begin


while [ $counter -lt $size ] ; do
        if [ $lagcheck -eq 1 ] ; then
          check_sbm
        fi

        if ((multi != 1)) ; then
            echo -en "Progress $counter/$size\r"

            echo "${line[$counter]}" | ${mysql_command} | sort -t: -nk2
            ret="${PIPESTATUS[0]}${PIPESTATUS[1]}${PIPESTATUS[2]}"
            if [ "${ret}" != "000" ]
                    then echo -e "problem on $((counter+1)) line of ${proc_file}:\n ${line[$counter]}" ; exit 1; fi
            counter=$(($counter+1))
        else
            echo  "Progress $counter/$size"
            echo "${line[$counter]}" | ${mysql_command} | sort -t: -nk2
            ret="${PIPESTATUS[0]}${PIPESTATUS[1]}${PIPESTATUS[2]}"
            if [ "${ret}" != "000" ]
                    then echo -e "problem on $((counter+1)) line of ${proc_file}:\n ${line[$counter]}" ; exit 1; fi
            counter=$(($counter+1))
        fi

done

#clean after work
  if [ -e ${sql_file} ] ; then rm -f ${sql_file} ; fi
  if [ -e ${proc_file} ] ; then rm -f ${proc_file} ; fi

