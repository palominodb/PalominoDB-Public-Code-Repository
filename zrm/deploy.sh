#!/bin/bash

code_base=$1
do_tarball=$2
tag=$3

if [[ -z $code_base ]]
then
	echo "Need a codebase!"
	exit 1
fi

if [[ -n $do_tarball ]]
then
  # Do stuff to make a tarball at tag X.
  if [[ -z $tag ]]
  then
    echo "Need a tag to make a tarball."
    exit 1
  fi
  prev_head=$(git branch | grep '^*' | awk '{print $2}')
  git stash
  #git checkout $tag
  mkdir $do_tarball-$tag
  cp -r plugins examples $do_tarball-$tag/
  git log . > $do_tarball-$tag/CHANGELOG.git
  cp README CHANGELOG $do_tarball-$tag/
  cp $do_tarball.spec $do_tarball-$tag/
  tar czvf $do_tarball-$tag.tgz  $do_tarball-$tag/
  rm -rf $do_tarball-$tag/
  #git checkout "$prev_head"
  git stash pop
  exit 0
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
