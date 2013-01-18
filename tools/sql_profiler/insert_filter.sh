#!/bin/bash
# insert_filter.sh
# Copyright (C) 2013 PalominoDB, Inc.
# 
# You may contact the maintainers at eng@palominodb.com.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

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
echo -e "$SedCnt"

sed -e "
s/SCRIPT_GIT_VERSION/$ScriptGitVer/ ;
s/^\(## BEGIN $Filter \)GIT_VERSION/\1$GitVer/ ;
/^## BEGIN $Filter/ a\ 
$SedCnt
" $Prog > $(echo $Prog | sed -e 's/.in//')
