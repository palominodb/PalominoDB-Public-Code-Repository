#!/bin/bash

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
