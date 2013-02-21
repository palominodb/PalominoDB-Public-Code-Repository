Python Nagios Plugins for monitoring MySQL-ZRM and Percona Xtrabackup
================================

Install Requirements
-------------------------------
`pip install -r requirements.txt`


check_xtrabackup_log
-------------------------------
Parses the xtrabackup log and looks for `completed OK!` which means that the last backup is OK. Otherwise, returns warning to Nagios.

Example Usage:  
`python check_xtrabackup_log.py -f xtrabackup-agent.log -d 10`

Usage: check_xtrabackup_log.py [options]

Options:  
  -f FILE, --file=FILE  path to Xtrabackup-agent log  
  -d DAYS, --days=DAYS  number of days backup shouldn't be older than  
  -v, --verbose         
  -H HOSTNAME, --hostname=HOSTNAME  
  -w WARNING, --warning=WARNING  
  -c CRITICAL, --critical=CRITICAL  
  -t TIMEOUT, --timeout=TIMEOUT  
  -h, --help            show this help message and exit  

check_zrm_backup
-------------------------------
Parses the output of mysql-zrm-reporter. If zrm-reporter shows backup as OK, plugin assumes it's ok. Otherwise, plugin returns warning to Nagios.

Example Usage:  
`python check_zrm_backup.py -p /var/lib/mysql-zrm/ -b backupset1,backupset2 -d 10`

Usage: check_zrm_backup.py [options]

Options:
  -d DAYS, --days=DAYS  number of days backup shouldn't be older than  
  -b BSETS, --backup-set=BSETS  
                        comma-separated backup sets to check  
  -p BDIR, --backup-dir=BDIR  
                        path to --destination for mysql-zrm-reporter  
  -v, --verbose         
  -H HOSTNAME, --hostname=HOSTNAME  
  -w WARNING, --warning=WARNING  
  -c CRITICAL, --critical=CRITICAL  
  -t TIMEOUT, --timeout=TIMEOUT  
  -h, --help            show this help message and exit  

