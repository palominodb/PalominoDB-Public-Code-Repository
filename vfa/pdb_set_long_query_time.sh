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

# At some point should add a clear slow_log option. So
# it's easier to review a window of time.

if [ -z $1 ];then
  echo "Error: usage $0 <minutes>"
  exit 1
else
  minutes=$1
fi

start_date=`date +"%Y%m%d%H%M%S"`
logfile=/var/log/pdb_set_long_query_time_${start_date}.log
start_unixtime=`date +"%s"`
count=0
max_pct_used=90

exec > >(tee -a $logfile) 2>&1

echo "Start unix time = $start_unixtime"

slow_query_log_file=`mysql -sNe 'show global variables like "slow_query_log_file"' |awk '{print $2}'`


if [ ! -e $slow_query_log_file ];then
  echo "Error: $slow_query_log_file doesn't exist."
  exit 1
else
  echo "Found slow_query_log_file=$slow_query_log_file"
  echo
fi

# Had to have awk convert the float returned from mysql to an int
long_query_time_before=`mysql -sNe 'show global variables like "long_query_time"' |awk '{printf "%3.3f",$2}'`
# DEBUG comment out after
# echo long_query_time_before=$long_query_time_before

echo "Flushing logs."
mysql -e 'flush logs'
echo
echo "Cp'ing slow query log file"
echo "cp ${slow_query_log_file} ${slow_query_log_file}_${start_date}"
cp ${slow_query_log_file} ${slow_query_log_file}_${start_date}
echo "Clearing the slow log for easier review."
echo "> ${slow_query_log_file}"
> ${slow_query_log_file}
echo

# Set long query time = 0
long_query_time_after=`mysql -sNe 'set global long_query_time=0;show global variables like "long_query_time"' |awk '{printf "%3.3f",$2}'`
# DEBUG
# echo long_query_time_after=$long_query_time_after

sleep_time_in_seconds=$(( $minutes * 60 ))
# DEBUG
# sleep_time_in_seconds=1

echo "The script has set long_query_time = $long_query_time_after. It will reset long_query_time = $long_query_time_before in"
echo "$minutes minutes (${sleep_time_in_seconds}s)."
echo

while [ ${count} -lt ${sleep_time_in_seconds} ]
do
  pct_used=`df -Ph  ${slow_query_log_file}  |grep -v Filesystem |awk '{print $5}' |sed 's/%//'`
  if [ $pct_used -gt ${max_pct_used} ];then
    echo "Error: pct_used = ${pct_used}. Clear up some space before running this script."
    break
  fi

  sleep 1
  count=$(( $count + 1 ))
done

long_query_time_final=`mysql -sNe "set global long_query_time=${long_query_time_before};show global variables like 'long_query_time'" |awk '{printf "%3.3f",$2}'`
echo long_query_time_final=$long_query_time_final

echo "The script has completed. Long_query_time has been set back to long_query_time=${long_query_time_final}."
echo
end_date=`date +"%Y%m%d%H%M%S"`
sample_slow_log="${slow_query_log_file}_${minutes}min_sample_${end_date}"
mysql -e 'flush logs'
echo
echo "Creating file slow query log file for analysis."
echo "cp ${slow_query_log_file} ${sample_slow_log}"
cp ${slow_query_log_file} ${sample_slow_log}
echo "Final clearing of the slow log for space."
echo "> ${slow_query_log_file}"
> ${slow_query_log_file}
echo

sample_slow_log_size=`du -ks ${sample_slow_log} |awk '{print $1}'`

echo "The sample slow query log, ${sample_slow_log}, is now ${sample_slow_log_size}."


# calculate growth rate of slow log

growth_rate=$(echo "scale=3; (${sample_slow_log_size} / ${sleep_time_in_seconds}) / 1024" | bc )

echo "The slow log grew at approximately ${growth_rate}M/s"


end_unixtime=`date +"%s"`

echo "End unixtime = ${end_unixtime}"


