#! /bin/bash

# Check on the health of couchbase and print stats out to a file
# Note the output file is removed at the beginning of every run

RESTPORT=8091
CBPATH=/opt/couchbase
NODE=()
BUCKET=()
BUCKET_PORT=()
BUCKET_TYPE=()
BUCKET_AUTHTYPE=()
BUCKET_PWD=()
MU=""

while [[ $1 = -* ]]; do
   arg=$1; shift

   case $arg in
     --help)
         echo "couchbase_sitescope.sh --host <ip of any cb node in the cluster> --user <Administrator> --pwd <Disney...> --log_file <cb_qapci.csv>";exit
         ;;
     --host)
         HOST=$1
         shift
         ;;
     --user)
         USER=$1
         shift
         ;;
     --pwd)
         PWD=$1
         shift
         ;;
     --log_file)
         LOGFILE=$1
         shift
         ;;
    esac
done

if [ -e $LOGFILE ]; then
  /bin/rm $LOGFILE
fi


log() {
    echo -e "$@" >> $LOGFILE
}

# first find out all the nodes in the cluster given this one
i=1
while read line
do
  NODE[$i]=$(echo $line|awk '{print $1;}')
  STATUS[$i]=$(echo $line|awk '{print $2,$3;}')
  # print the nodes and status to the log file
  log CBSTATUS,${NODE[$i]},${STATUS[$i]}
  (( i++ ))
done < <($CBPATH/bin/couchbase-cli server-list -c $HOST:$RESTPORT -u $USER -p $PWD|cut -d '@' -f2| awk '{print $1, $3,$4;}')

# have to pull info about the buckets first
IFS='' # preserves the leading space
i=0
while read line
do
  if [[ ! "$line" == " "* ]]; then
    (( i++ ))
    BUCKET[$i]=$(echo $line|awk '{print $1;}')
    #echo "$i ${BUCKET[$i]}"
  elif [[ "$line" == " bucketType"* ]]; then
    BUCKET_TYPE[$i]=$(echo $line|awk '{print $2;}')
    #echo "$i ${BUCKET_TYPE[$i]}"
  elif [[ "$line" == " authType"* ]]; then
    BUCKET_AUTHTYPE[$i]=$(echo $line|awk '{print $2;}')
    #echo "$i ${BUCKET_AUTHTYPE[$i]}"
  elif [[ "$line" == " proxyPort"* ]]; then
    BUCKET_PORT[$i]=$(echo $line|awk '{print $2;}')
    #echo "$i ${BUCKET_PORT[$i]}"
  elif [[ "$line" == " saslPassword"* ]]; then
    BUCKET_PWD[$i]=$(echo $line|awk '{print $2;}')
    #echo "$i ${BUCKET_PWD[$i]}"
  fi 
done < <($CBPATH/bin/couchbase-cli bucket-list -c $HOST:$RESTPORT)


#temp OOM per sec: should be 0.  If it isn't, you must add a new node.

#cache miss ratio: should be 0.  If it isn't, you must add new node.

#vbucket resources
# resident ratio: ideal if 100% resident, since that means everything is in memory.  active should always be 100%, though it.s kind of okay if replica is not. CLARIFY
# ejections per second: should be 0.  Shows ejections from working memory.  Add a node. CLARIFY
# active vbuckets should be 1024 (default).  It might drop down to 1023 during a rebalance but should be 1024 otherwise.  Lower numbers are normal during warmup, but at other times indicate that there is an issue with a bucket on a node.

#disk queues
# drain rate should be equal to or higher than fill rate.
# items should be low.  Under 1000 is great.  
# average age should be < 0.01 seconds.
# If the disk write queue hits 1m, the internode tap streams start backing off, causing replication to stop. Check the drain rates to make sure it.s writing something to disk.   If it's 0, the node will need to be taken out. If it's > 0, then it.s about adding nodes.

#tap queues :similar to disk queues but for internode communication.  A key indicator of replication health.


# cbstats gives info per node per bucket, so have to sweep through all the nodes
# sweep through by bucket by node

for (( i = 1 ; i <= ${#BUCKET[@]} ; i++ )) do
  if [[ ${BUCKET_AUTHTYPE[$i]} == "sasl" ]] && [[ -z ${BUCKET_PORT[$i]} ]]   ; then
    BUCKET_PORT[$i]="11210"
  fi

  CACHEMISS_PCT=0 # cache misses percent
  MEMUSED_PCT=0 # memory used percent
  DISKQUEUE=0     # disk queue
  HW=0     # high water mark
  GH=0     # get hits
  GM=0     # get misses
  MU=0     # memory used
  OOM_ERRORS=0     # temporary out of memory errors

  for (( j = 1 ; j <= ${#NODE[@]} ; j++ )) do
    while read line
    do
      if [[ "$line" == *ep_queue_size* ]]; then
        DISKQUEUE=`echo $line| awk 'BEGIN {FS=":"}{print $2}'|tr -d ' ' `
      elif [[ "$line" == *ep_mem_high_wat* ]]; then
        HW=`echo $line|awk 'BEGIN {FS=":"}{print $2}'|tr -d ' '`
      elif [[ "$line" == *get_hits* ]]; then
        GH=`echo $line|awk 'BEGIN {FS=":"}{print $2}'|tr -d ' '`
      elif [[ "$line" == *get_misses* ]]; then
        GM=`echo $line|awk 'BEGIN {FS=":"}{print $2}'|tr -d ' '`
      elif [[ "$line" == *mem_used* ]]; then
        MU=`echo $line|awk 'BEGIN {FS=":"}{print $2}'|tr -d ' '`
      elif [[ "$line" == *ep_tmp_oom_errors* ]]; then
        OOM_ERRORS=`echo $line|awk 'BEGIN {FS=":"}{print $2}'|tr -d ' '`
      fi
    done < <($CBPATH/bin/cbstats ${NODE[$j]}:${BUCKET_PORT[$i]} all ${BUCKET[$i]} ${BUCKET_PWD[$i]} | egrep "ep_queue_size|ep_mem_high_wat|mem_used|get_hits|get_misses|ep_tmp_oom_errors|ep_vb_total")

    if [ "$HW" -gt "0" ]; then
      MEMUSED_PCT=$(echo "scale=2; $MU/$HW*100" |bc -l)
    else
      MEMUSED_PCT=0
    fi

    if [[ ${BUCKET_TYPE[$i]} == "membase" ]] ; then
      MT=$(( $GM+$GH ))
      if [ "$MT" -gt "0" ]; then
        CACHEMISS_PCT=$(echo "scale=2; $GM/$MT*100" |bc -l)
      else
        CACHEMISS_PCT=0
      fi
      log "CACHEMISS_PCT" ${BUCKET[$i]} ${NODE[$j]},$CACHEMISS_PCT
      #echo "CACHEMISS_PCT ${NODE[$j]} ${BUCKET[$i]} $CACHEMISS_PCT"
      log "DISKQUEUE" ${BUCKET[$i]} ${NODE[$j]},$DISKQUEUE
    fi
    log "MEMUSED_PCT" ${BUCKET[$i]} ${NODE[$j]},$MEMUSED_PCT
    log "OOM_ERRORS" ${BUCKET[$i]} ${NODE[$j]},$OOM_ERRORS
  done 


done

