#!/bin/bash

Filter=$1
Prog=$2

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
