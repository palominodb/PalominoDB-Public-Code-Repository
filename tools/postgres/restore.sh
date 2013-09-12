#!/bin/bash
# Provide a simple way to implement  delayed server using restore command
# Written by Emanuel for PalominoDB and trainig purposes. Use at your own risk. 

# Example line in teh recovery.conf
#restore_command = '/var/lib/pgsql9/PalominoDB-Public-Code-Repository/tools/postgres/restore.sh  -D /full/path/%f -x %p -d 1'               # e.g. 'cp /mnt/server/archivedir/%f %p'

# I recommend to use pg_archivecleanup for clean up the log files instead this file.

#TODO
# * Would be nice to add warnings when the source directory is about to explote
# * Send mail when is failing or file not found
# * Check if the file was currently restored
# * Translate this on Python. This script was started for a simply project.

TMPDIR=/tmp
FLAGFILE=$TMPDIR/$(basename $0).flag
TIMEFILE=$TMPDIR/time_checkpoint
DEFHOURS=24
DELAY=$DEFHOURS
VERBOSE=""

_usage_()
{
  cat <<EOF

= $0 =
  Where I use this script? On the restore.conf. How?
   restore_command = '$0 -D /var/data/xlogs_from_a_master/%f -x %p -d <hours_to_delay>'
  

  Parameters:
   -D <my_next_wal_files>         This is a mandatory parameter. Should be the full path to the file, i.e.: /mnt/server/archivedir/%f
   -d <hours_to_delay>            by default is $DEFHOURS
   -x <directory_to_copy>         This is a mandatory parameter.  i.e.: -x %p 
   -c                             Remove the source file. Not recommended, use pg_archivecleanup (this script doesn't check if was already restored, yet)
   -v                             Verbose the copy and the delete (if specified). You shouldn't need this, actually Postgres will notify you when a file has been recovered.                         

EOF
}

while getopts 'D:d:x:h' OPTION 
do
  case $OPTION in
    D)
      RECFILE=$OPTARG
      ;;
    x)
      XLOGDIR=$OPTARG
      ;;
    d)
      DELAY=$OPTARG
      ;;
    h)
      _usage_
      exit 0
      ;;
    c)
      CLEAN_DA_FILE=1
      ;;
    v)
      VERBOSE="-v"
      ;;
    *)
      exit 1
      ;;
  esac
done


[ $RECFILE ] || { echo "$(date) The parameter -D is mandatory for specify the source directory of the archived WALs" ; exit 2 ; }
[ -f $RECFILE ] || { echo "$(date) The file doesn't exist, please check the path: $RECFILE" ; exit 4 ; }


_DATE_=$(date -d "$DELAY hours ago")
touch -d "$_DATE_" $TIMEFILE || { echo "$(date) Check permissions on the $TMPDIR folder" ; exit 5 ; }


if [ $RECFILE -ot $TIMEFILE  ] && [ -f $RECFILE ]
then
  rm -f $TIMEFILE
  cp -f $VERBOSE $RECFILE $XLOGDIR || { echo "$(date) Some error ocurred while copying. $?" ; exit 10 ;  }
fi

if [ $CLEAN_DA_FILE ] 
then
  rm -f $VERBOSE  $RECFILE || { echo "$(date) An error ocurred when trying to delete the source file $RECFILE" ; exit 20 ; }
fi

#Oh ha!
exit 0






