#!/bin/bash
# Provide a simple way to implement  delayed server using restore command

# NOTES:
# -d (delay option) is in hours.

#Mandatory parameters:
# archive directory, XLOG DIR, DELAY hours

# Example line in teh recovery.conf
#restore_command = '/var/lib/pgsql9/PalominoDB-Public-Code-Repository/tools/postgres/restore.sh  -D /full/path/%f -x %p -d 1'               # e.g. 'cp /mnt/server/archivedir/%f %p'

# need to check if full path in the -x optoin needed

# I recommend to use pg_archivecleanup for clean up the log files instead this file.


TMPDIR=/tmp
#_T_=$(basename ${0})
FLAGFILE=$TMPDIR/$(basename $0).flag
TIMEFILE=$TMPDIR/time_checkpoint
DEFHOURS=24
DELAY=$DEFHOURS
#RECOVERY_DIR=/mnt/server/archivedir

_usage_()
{
  cat <<EOF

= $0 =
  Where I use this script? On the restore.conf. How?
   restore_command = '$0 -D /var/data/xlogs_from_a_master/%f -x %p'

  Parameters:
   -D <my_next_wal_files>         This is a mandatory parameter. Should be the full path to the file, i.e.: /mnt/server/archivedir/%f
   -d <hours_to_delay>            by default is $DEFHOURS
   -x <directory_to_copy>         This is a mandatory parameter.  i.e.: -x %p 

EOF
}
while getopts 'D:d:x:' OPTION 
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
      ;;
    *)
      exit 1
      ;;
  esac
done

[ $RECFILE ] || { echo "$(date) The parameter -D is mandatory for specify the source directory of the archived WALs" ; exit 2 ; }


_DATE_=$(date -d "$DELAY hours ago")
touch -d "$_DATE_" $TIMEFILE || { echo "Check permissions on the $TMPDIR folder" ; exit 5 ; }

#find $RECDIR -mtime $DELAYh -exec cp {} $XLOGDIR \;    ||  { echo "An error ocurred: $?" ; exit 3 ;  }

if [ $RECFILE -ot $TIMEFILE  ] && [ -f $RECFILE ]
then
  rm -f $TIMEFILE
  cp $RECFILE $XLOGDIR || { echo "Some error ocurred while copying. $?" ; exit 10 ;  }
fi


exit 0






