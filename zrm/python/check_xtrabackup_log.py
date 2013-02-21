#!/usr/bin/python -w

from datetime import date, datetime, timedelta
import fileinput
from optparse import OptionParser
import re
import calendar
import time
import subprocess
import sys
import os
       
from pynagios import Plugin, Range, Response, make_option

class CheckXtraBackupLog(Plugin):
    xtrabackup_log = make_option("-f", "--file", dest="xtrabackup_log",
                        help="path to Xtrabackup-agent log", metavar="FILE")
    days = make_option('-d', '--days', dest='days', help="number of days backup shouldn't be older than")
    
    def check(self):
        self.options.warning = self.options.warning if self.options.warning is not None else Range('0')
        
        status_arr = []
        for i in reversed(self.check_log(self.options.xtrabackup_log, self.options.days)):
            status_arr.append(i)
        
        if status_arr[0] == 0:
            return self.response_for_value(0, 'last backup is OK')
        else:
            return self.response_for_value(1, 'Problem found with last backup')
        
    def check_log(self, logfile, days_to_check):
        p1 = re.compile('msg (\\d*)\..*prints "completed OK.*\..*')
        p2 = re.compile('.*innobackupex-.*: completed OK!.*')

        lines = []
        with open(logfile,'r') as f:
            lines = f.readlines()

        chk_for_start = 1
        chk_for_end = 0
        backup_status = []
        for line in lines:
            m1 = p1.match(line)
            if m1:
                backdate = datetime.fromtimestamp(float(m1.group(1)))
                if backdate.timetuple() > (datetime.now() - timedelta(days=int(days_to_check))).timetuple():
                    if chk_for_end == 1 :
                        chk_for_end = 0
                        backup_status.append(2)

                    chk_for_start = 0
                    chk_for_end = 1

            if chk_for_end == 1:
                m2 = p2.match(line)
                if m2:
                    chk_for_end = 0
                    chk_for_start = 1
                    backup_status.append(0)
        return backup_status

if __name__ == "__main__":
    # Instantiate the plugin, check it, and then exit
    CheckXtraBackupLog().check().exit()
