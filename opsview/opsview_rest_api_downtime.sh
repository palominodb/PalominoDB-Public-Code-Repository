#!/bin/bash
#
# opsview_rest_api_downtime.sh
# create or delete downtime for a single host using opsview curl rest api
#
# Copyright 2012 PalominoDB, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

CURL=/usr/bin/curl
OPSVIEW_HOSTNAME="opsview.example.com" # HOSTNAME for opsview admin interface
USERNAME=apiuser # may change depending on opsview configuration
URL="/rest/downtime" # this is the default, unlikely this URL would change

hours_of_downtime=2 # change to whatever you need for default

usage()
{
    echo "Usage: $0 -p <opsview apiuser password> -h <host> -c (create|delete) [-t <hours_of_downtime>]"
    exit 1
}

while getopts p:h:t:c: opt
do
    case $opt in 
      p) password=$OPTARG;;
      h) host=$OPTARG;;
      t) hours_of_downtime=$OPTARG;;
      c) command=$OPTARG;;
      \?) usage;;
    esac
done


if [ "x$password" = "x" ] || [ "x$host" = "x" ] || [ "x$command" = "x" ]
then
    usage
fi

# LOGIN

token_response=`$CURL -s -H 'Content-Type: application/json' https://$OPSVIEW_HOSTNAME/rest/login -d "{\"username\":\"$USERNAME\",\"password\":\"$password\"}"`
token=`echo $token_response | cut -d: -f 2 | tr -d '"{}'`
if [ ${#token} -ne 40 ]
then
    echo "$0: Invalid apiuser login. Unable to $command downtime."
    exit 1
fi
    

if [ "$command" = "create" ]
then
    # create downtime - POST
    starttime=`date +"%Y/%m/%d %H:%M:%S"` 
    endtime=`date +"%Y/%m/%d %H:%M:%S" -d "$hours_of_downtime hours"`
    comment="$0 api call"
    data="{\"starttime\":\"$starttime\",\"endtime\":\"$endtime\",\"comment\":\"$comment\"}"
    result=`$CURL -s -H "Content-Type: application/json" -H "X-Opsview-Username: $USERNAME" -H "X-Opsview-Token: $token" https://$OPSVIEW_HOSTNAME$URL?host=$host -d "$data"`
    exit_status=$?
else
    # delete downtime - DELETE
    params="host=$host"
    result=`$CURL -s -H "Content-Type: application/json" -H "X-Opsview-Username: $USERNAME" -H "X-Opsview-Token: $token" -X DELETE https://$OPSVIEW_HOSTNAME$URL?$params`
    exit_status=$?
fi
echo "$result" | grep $host > /dev/null
host_in_output=$?
if [ "$exit_status" -ne "0" ] || [ "$host_in_output" -ne "0" ]
then
  echo "Unable to $command downtime for $host.  Result of call:"
  echo $result
  exit 1
fi

