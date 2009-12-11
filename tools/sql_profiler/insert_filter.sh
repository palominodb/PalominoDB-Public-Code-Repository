#!/bin/bash

usage() {
  echo "$0 <filter filename> <source file>"
  exit 1
}

Filter=$1
Prog=$2

case "$Filter" in
  "-h")
    usage
    ;;
  "-help")
    usage
    ;;
  "--help")
    usage
    ;;
  "-?")
    usage
    ;;
esac

if [[ -z "$Filter" ]]; then
  usage
fi

Content="$(cat $Filter)"
GitVer=$(../../util/gitver.sh $Filter)
ScriptGitVer=$(../../util/gitver.sh $Prog)

SedCnt="$(echo "$Content" | sed -e 's/^\([ ]\)/\\\1/; s/$/\\/')"

sed -e "
s/SCRIPT_GIT_VERSION/$ScriptGitVer/ ;
s/^\(## BEGIN $Filter \)GIT_VERSION/\1$GitVer/ ;
/^## BEGIN $Filter/ a\
$SedCnt
" $Prog > $(echo $Prog | sed -e 's/.in//')
