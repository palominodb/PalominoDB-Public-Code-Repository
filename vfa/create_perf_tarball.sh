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

tarball_dir=/tmp/perf_tarball
date=`date +"%Y%m%d%H%M%S"`
tarball_file=perf_tarball_${date}.tgz
perf_dest_dir="/tmp"

# Removing previous versions of perf_tarball.
rm -vf ${perf_dest_dir}/perf_tarball_[0-9][0-9]*.tgz

mkdir -p /tmp/perf_tarball 2> /dev/null

# Deprecated. Removing after there is a replacement
# cd ${HOME}/git/ServerAudit/scripts/
# cp call_pt-stalk.sh    ${tarball_dir}
# cp gen_stalk_report.sh ${tarball_dir}

cd ${HOME}/git/dba/
cp * ${tarball_dir}

cd /tmp
tar czvf ${tarball_file} perf_tarball
echo ${tarball_file} 

