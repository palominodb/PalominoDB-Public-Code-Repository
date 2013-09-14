#!/bin/bash
#
# Purpose: Backup a Postgres DB
#
# Encryption Workflow:
# - The encryption mechanism was taken from this link: http://blog.altudov.com/2010/09/27/using-openssl-for-asymmetric-encryption-of-backups/
# 1. Generate private and public keys for encryption
#   openssl req -x509 -nodes -days 100000 -newkey rsa:2048 -keyout private.pem -out public.pem -subj '/'
# 2. The script may now be run by passing the generate public key. The script uses openssl smime command for encryption
#   openssl smime -encrypt -aes256 -binary -outform DER -out backups.zip.enc public.pem
# 3. To decrypt the backup, use the command
#   openssl smime -decrypt -in backups.zip.enc -binary -inform DER -inkey private.pem -out backups.zip.dec
#

# Useful link information https://kb.wisc.edu/middleware/page.php?id=4064

# Dependencies
#
# Please install the awscli, instructions at: http://aws.amazon.com/cli/
#
# You shuold have configured the key_id and secret on your environment for upload the files to S3

# TODO
# ADding awscli compatibility
# Flexibility for the dump tool
 
VERSION="0 alpha"
 
## Configuration variables
S3_FOLDER="s3://backups"
AWS_DEFAULT_REGION="us-west-1"
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=

#Postgres conf
PGBINHOME=/usr/bin
PGPSQL=/usr/bin/psql
DBUSER=postgres

#Email conf
EMAIL='dummy@domain.com'
MAIL=/home/db_sync/bin/send_gmail.py
MAIL_ME=1      # 0 means no mail, only log
MAIL_IF_OK=1   # Set to 1 if you want mails if backup is OK:

# Security
PUB_KEY_PATH=/default/key/location

#Fine tuning
COMP_LEVEL=9   # We recommend the maximum level of compression


################### Don't change upon this line ################################
 
BACKUP_STATUS=0
JUMP_TO=48
BACKUP_NAME=`hostname|perl -p -e 's/\s//g'`
DATE=`date +%Y-%m-%d-%H`
#If backup is ok it will turn into an OK message
SUBJECT="Postgres Backup Error -- $(hostname)"
EMAILMESSAGE="/tmp/emailmessage.txt"
 
 
usage()
{
cat << EOF
usage: $0 options
 
Postgres backup script. Version $VERSION
 
Encryption Workflow:
- The encryption mechanism was taken from this link: http://blog.altudov.com/2010/09/27/using-openssl-for-asymmetric-encryption-of-backups/
 1. Generate private and public keys for encryption
   openssl req -x509 -nodes -days 100000 -newkey rsa:2048 -keyout private.pem -out public.pem -subj '/'
 2. The script may now be run by passing the generate public key. The script uses openssl smime command for encryption
   openssl smime -encrypt -aes256 -binary -outform DER -out backups.zip.enc public.pem
 3. To decrypt the backup, use the command
   openssl smime -decrypt -in backups.zip.enc -binary -inform DER -inkey private.pem -out backups.zip.dec
 
OPTIONS:
    -h                      Show this message
    -b PUB_KEY_PATH         Public key for encryption.               Default: $PUB_KEY_PATH
    -e EMAIL                Email address where errors will be sent. Default: $EMAIL
    -f S3_FOLDER            S3 folder to put the backup. Used the S3 path.
                            I.e.: s3://bakcup_floder                 Default: $S3_FOLDER
    -u DBUSER               Which user connects to Postgres          Default: $DBUSER
    -r REGION               Region for S3 upload                     Default: $REGION
    -c <integer>            Compression level                        Default: $COMP_LEVEL
    -l FILGE_LOG            Set a log file                           Default: $LOG_FILE
    -M                      Do not send mail, just log               Default: mail is enabled
EOF
}
 
 
 
mail()
{
    [ $BACKUP_STATUS -lt "2" ] && SUBJECT="Postgres Backup OK -- $(hostname)" || echo $1 > $EMAILMESSAGE
    parsed_body=$(cat $EMAILMESSAGE | sed ':a;N;$!ba;s/\n/\\nnn/g')
    python $MAIL -s "$SUBJECT" -a "$EMAIL" -b "$parsed_body"
    rm $EMAILMESSAGE
    exit $2
}
 
 
 
while getopts 'he:b:f:c:r:u:l:M' OPTION
do
    case $OPTION in
        h)
            usage
            exit
            ;;
        b)
            PUB_KEY_PATH=$OPTARG
            ;;
        e)
            EMAIL=$OPTARG
            ;;
        f)
            S3_FOLDER=$OPTARG
            ;;
        c)
            COMP_LEVEL=$OPTARG
            ;;
        C)
            CHECK_KEYS=1
            ;;
        P)
            PRIVATE_KEY_PATH=$OPTARG
            ;;
        r)
            AWS_DEFAULT_REGION=$OPTARG
            ;;
        u)
            DBUSER=$OPTARG
            ;;
        l)
            LOG_FILE=$OPTARG
            ;;
        M)
            MAIL_ME=0
            ;;
        *)
            echo "Invalid option, please check"
            exit 4
            ;;
    esac
done
 
if [ -z $EMAIL ] || [ -z $PUB_KEY_PATH ] 
then
    echo "-e, -b and arguments are required."
    exit 11
fi
 
 
#Check if public key file exists
if [ ! -e $PUB_KEY_PATH ]
then
    echo "File '$PUB_KEY_PATH' does not exist."
    exit 22
fi
 
#Check if the backup name is set
if test -z $BACKUP_NAME
then
    echo "Bad backup name"
    exit 1
fi
 
if [ $CHECK_KEYS -a ! -f $PRIVATE_KEY_PATH ]
then
  echo "You need to specify the private key path to check"
  exit 15
fi

#Check key health here

#Check if pg_dumpall command exists
if ! test -e ${PGBINHOME}/pg_dumpall
then
    echo "Can't find pg_dumpall!"
    exit 2
fi
 
#Check if S3_FOLDER exists
aws ls  $S3_FOLDER > /dev/null || { echo "S3 folder provided doesn't exist or not privileges are set" ; exit 33 ; }
 
#Check if we have connection to the database
$PGPSQL -U$DBUSER  template1 -o /tmp/trash -Atc 'select 0'
if [ ! "$?" -eq "0" ] ; then
    echo "Check if the database is running or if the user has privileges."
    exit 3
fi


#Check the fingerprint of the public key. Useful to reference when restore.

PUB_KEY_MD5=$(openssl x509 -noout -modulus -in $PUB_KEY_PATH | openssl md5) 
 

#Backup command
$PGBINHOME/pg_dumpall -U $DBUSER | gzip -$COMP_LEVEL -c | openssl smime -encrypt -aes256 -binary -outform DER -out backups.zip.enc $PUB_KEY_PATH  || { BACKUP_STATUS=5 ; echo "failed to dump database" ; mail "failed to dump database" $BACKUP_STATUS ;   }
 
  echo "Backup Size:"  >> $EMAILMESSAGE
  du -h backups.zip.enc | cut -f1 >> $EMAILMESSAGE
  echo "" >> $EMAILMESSAGE
  echo "Public key hash $PUB_KEY_MD5 " >> $EMAILMESSAGE
  echo "" >> $EMAILMESSAGE
  echo "List of databases in the backup:" >> $EMAILMESSAGE
  $PGPSQL -U$DBUSER  template1 -l >> $EMAILMESSAGE
 
#echo uploading to $BACKUP_NAME.$DATE.zip.enc
perl $AWS put $S3_FOLDER/$BACKUP_NAME.$DATE.zip.enc backups.zip.enc
if [ "$?" -ne "0" ] ;
then
  echo "failed to upload backup file"
  BACKUP_STATUS=10
  mail "failed to upload backup file" $BACKUP_STATUS
fi
 
 
#echo $BACKUP_NAME $JUMP_TO
for old_one in $(perl $AWS ls $S3_FOLDER $BACKUP_NAME | awk -F '|' "\\$7 ~ /$BACKUP_NAME/ { print \\$7 }"| sort -r -n | tail -n +$JUMP_TO)
do
  if test -z $old_one
  then
    echo got an empty name for old_one. not deleting
  else
    echo deleting :${old_one}:
    perl $AWS delete $S3_FOLDER/$old_one || { BACKUP_STATUS=12 ; echo "failed to delete an old backup $old_one" ; mail "failed to delete an old backup $old_one" $BACKUP_STATUS ;  }
  fi
done
 
 
 
 
if [ $MAIL_IF_OK -eq "1" -a  $BACKUP_STATUS -lt "2" ]
then
  BACKUP_STATUS=1
  mail "Backup Done Sucessfully" 0
fi
