#!/bin/bash
# gitver.sh
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

# List of possible sha1 sum programs
SHASUM_BINS="sha1sum shasum"
SHASUM=""

File="$1"

if [[ ! -f "$File" ]]; then
  echo "No such file."
  exit 1
fi

if [[ ! -r "$File" ]]; then
  echo "File is not readable."
  exit 1
fi

linux_stat() {
  File="$1"
  stat -c %s "$File"
}

darwin_stat() {
  File="$1"
  stat -f %z "$File"
}

pstat() {
  File=$1
  platform=$(uname)
  if [[ "x$platform" = "xLinux" ]]; then
    linux_stat $1
  elif [[ "x$platform" = "xDarwin" ]]; then
    darwin_stat $1
  else
    echo -1
  fi
}

for prog in $SHASUM_BINS; do
  SHASUM=$(which $prog)
  if [[ $? -eq 0 ]]; then
    break
  fi
done

if [[ -z "$SHASUM" ]]; then
  echo "Could not find a suitable sha1 sum program."
  echo "This is required for $0 to operate."
  exit 1
fi

Size=$(pstat "$File")

if [[ $Size -eq -1 ]]; then
  echo "Unable to determine size of given file."
  echo "Required to compute the git-ver."
  exit 1
fi

( echo -n -e "blob $Size\0" ; cat "$File" ) | $SHASUM | egrep -o "^[a-f0-9]{40}"
