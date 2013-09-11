#!/bin/bash
# Provide a simple way to implement  delayed server using restore command

# NOTES:
# -d (delay option) is in hours.

#Mandatory parameters:
# archive directory, XLOG DIR, DELAY hours

# Example line in teh recovery.conf
#restore_command='delayed_restore.sh -D /mnt/server/achivedir -x %p'   

# need to check if full path in the -x optoin needed

# I recommend to use pg_archivecleanup for clean up the log files instead this file.


TMPDIR=/tmp
#_T_=$(basename ${0})
FLAGFILE=$TMPDIR/$(basename $0).flag
TIMEFILE=$TMPDIR/time_checkpoint
DEFHOURS=24
DELAY=$DEFHOURS

# restore_command
#
# specifies the shell command that is executed to copy log files
# back from archival storage.  The command string may contain %f,
# which is replaced by the name of the desired log file, and %p,
# which is replaced by the absolute path to copy the log file to.
#
# This parameter is *required* for an archive recovery, but optional
# for streaming replication.
#
# It is important that the command return nonzero exit status on failure.
# The command *will* be asked for log files that are not present in the
# archive; it must return nonzero when so asked.
#
# NOTE that the basename of %p will be different from %f; do not
# expect them to be interchangeable.
#
#restore_command = ''           # e.g. 'cp /mnt/server/archivedir/%f %p'

_usage_()
{
  cat <<EOF

= $0 =
  Where I use this script? On the restore.conf. How?
   restore_command = '$0 -D /var/data/xlogs_from_a_master/ -x %p'

  Parameters:
   -D <where_are_my_wal_files>    This is a mandatory parameter
   -d <hours_to_delay>            by default is $DEFHOURS
   -x <directory_to_copy>         an example will be: -x %p 

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
    *)
      exit 1
      ;;
  esac
done

[ $RECFILE ] || { echo "The parameter -D is mandatory for specify the source directory of the archived WALs" ; exit 2 ; }

_DATE_=$(date -d "$DELAY hours ago")
touch -d "$_DATE_" $TIMEFILE || { echo "Check permissions on the $TMPDIR folder" ; exit 5 ; }

find $RECDIR -mtime $DELAYh -exec cp {} $XLOGDIR \;    ||  { echo "An error ocurred: $?" ; exit 3 ;  }

if [ $RECFILE -ot $TIMEFILE ]
then
  rm -f $TIMEFILE
  cp $RECFILE $XLOGDIR || { echo "Some error ocurred while copying. $?" ; exit 10 ;  }
fi


exit 0





