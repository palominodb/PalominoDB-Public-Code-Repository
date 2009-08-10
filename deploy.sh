#!/bin/bash

code_base=$1

if [[ -z $code_base ]]
then
	echo "Need a codebase!"
	exit 1
fi

HOSTS="testdb1 testdb2"
for host in $HOSTS
do
	rsync -avP $code_base/ root@$host:/usr/share/mysql-zrm/plugins/
	ssh root@$host 'chown -R mysql:mysql /usr/share/mysql-zrm/plugins'
	ssh root@$host 'chown -R mysql:mysql /etc/mysql-zrm'
	ssh root@$host 'mkdir -p /mysqlbackups && chown -R mysql:mysql /mysqlbackups'
	ssh root@$host 'chown -R mysql:mysql /var/log/mysql-zrm'
done
