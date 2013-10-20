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

dest_dir=/usr/local/palominodb/scripts
tarball_dir=`pwd`
mysql_conf_dir=/etc/mysql
# Change this if you have a nonstandard location for the my.cnf files.
mysql_site_specific_conf_dir=/data/admin/conf

mkdir -p ${dest_dir} 2> /dev/null
cd ${dest_dir}

cp ${tarball_dir}/* ${dest_dir}

# Locations searched for
# /data/admin/conf/my-*33*.cnf
# /data/admin/conf/my.cnf
# /etc/mysql/my.cnf
# /etc/mysql/my-*33*.cnf
# /etc/my.cnf

# Pattern to find my.cnf files
mysql_conf_regex="my-*33*.cnf"


# /etc/mysql/my-*33*.cnf
if [ `ls ${mysql_conf_dir}/${mysql_conf_regex} 2> /dev/null |wc -l` -lt 1 ];then
  # /data/admin/conf
  if [ -d ${mysql_site_specific_conf_dir} ];then
    mysql_conf_dir=${mysql_site_specific_conf_dir}
    # /data/admin/conf/my.cnf
    if [ -e ${mysql_site_specific_conf_dir}/my.cnf ];then
      mysql_conf_regex="my.cnf"
    fi
  # /etc/mysql/my.cnf
  elif [ -e ${mysql_conf_dir}/my.cnf ];then
    mysql_conf_regex="my.cnf"
  # /etc/my.cnf
  elif [ -e /etc/my.cnf ];then
    mysql_conf_dir="/etc"
    mysql_conf_regex="my.cnf"
    
  else
    echo "Error: problem finding my.cnf files in ${mysql_conf_dir}"
    exit 1
  fi
fi

for conf in `ls ${mysql_conf_dir}/${mysql_conf_regex} |sort`
do
  if [[ "${conf}" =~ my.cnf ]];then
    echo $conf:$(echo $conf |awk -F"/" '{print $NF}'|sed "s/my.cnf/3306/"):Y:N
  else
    echo $conf:$(echo $conf |awk -F"/" '{print $NF}'|sed "s/my-//;s/my-m//;s/.cnf//"):Y:N
  fi
done > ${mysql_conf_dir}/vfatab

if [ "${mysql_conf_dir}" != "/etc" ];then
  rm -f /etc/vfatab
  ln -s ${mysql_conf_dir}/vfatab /etc/vfatab
fi

# Check for bashrc and add dba alias if it doesn't already exist

if [ -e ${HOME}/.bashrc ];then
  if [ `grep "alias dba" ${HOME}/.bashrc | wc -l` -lt 1 ];then
    echo >> ${HOME}/.bashrc
    echo "# Added by palominodb for vfa_lib.sh `date`" >> ${HOME}/.bashrc
    echo "alias dba=\"source ${dest_dir}/vfa_lib.sh\"" >> ${HOME}/.bashrc

  fi
fi

echo "Install complete."
