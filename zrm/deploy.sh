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
  tar_dir=$do_tarball-$tag
  mkdir $tar_dir
  cp -r plugins examples $tar_dir/
  git log . > $tar_dir/CHANGELOG.git
  cp README CHANGELOG $tar_dir/
  cp $do_tarball.spec $tar_dir/
  cp -r freebsd $tar_dir
  cp -r debian $tar_dir
  tar czvf $tar_dir.tgz  $tar_dir/
  rm -rf $tar_dir/
  git stash pop
  exit 0
fi
