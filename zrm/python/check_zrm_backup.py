#!/usr/bin/python -w
# check_zrm_backup.py
#
# Copyright (C) 2010-2013 PalominoDB, Inc.
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

import calendar
import fileinput
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime, timedelta

from BeautifulSoup import BeautifulSoup        
from pynagios import Plugin, Range, Response, make_option

class CheckZrmBackup(Plugin):
    days = make_option('-d', '--days', dest='days', help="number of days backup shouldn't be older than")
    bsets = make_option('-b', '--backup-set', dest='bsets', help="comma-separated backup sets to check", default="allsets")
    bdir = make_option('-p', '--backup-dir', dest="bdir", help="path to --destination for mysql-zrm-reporter")
    
    def check(self):
        self.options.warning = self.options.warning if self.options.warning is not None else Range('0')
        
        days = int(self.options.days)
        today = date.today()
        daynr = today.timetuple()[2]
        
        l0_broken_list = []
        l1_broken_list = []
        bset_msgs = {}
        exit_code = 0
        
        if self.options.bsets == 'allsets':
            bsets = self.get_backupset_list(self.options.bdir)
        else:
            bsets = self.options.bsets.split(',')
            
        for bset in bsets:
            ret = self.check_log(bset, self.options.bdir, days)
            msgs = []
            if len(ret[0]) == 0:
                exit_code = 1
                msgs.append('No level 0 backup found')
            elif ret[0][0] == 0:
                msgs.append('Level 0 backup is OK')
            else:
                exit_code = 1
                msgs.append('Level 0 backup is broken')
                
            if len(ret[1]) == 0:
                exit_code = 1
                msgs.append('No level 1 backup found')
            elif ret[1][0] == 0:
                msgs.append('Level 1 backup is OK')
            else:
                exit_code = 1
                msgs.append('Level 1 backup is broken')
            
            bset_msgs.update({bset:msgs})
        msg = '\n'
        for k,v in bset_msgs.items():
            msg += '%s\n' % (bset)
            for bset_msg in v:
                msg += '\t- %s\n' % (bset_msg)
        
        return self.response_for_value(exit_code, msg)
    
    def get_backupset_list(self, bdir):
        bsets = []
        cmd = subprocess.Popen('/usr/bin/mysql-zrm-reporter --fields backup-set --noheader --destination %s 2> /dev/null' % (self.options.bdir),
                            shell=True, stdout=subprocess.PIPE)
        lines = cmd.stdout.readlines()
        lines = [line.strip() for line in lines]
        bsets = sorted(set(lines))
        return bsets
        
    def check_log(self, bset, bdir, days_to_check):
        lev0 = []
        lev1 = []
        #Use --type html for easier parsing
        cmd = subprocess.Popen('/usr/bin/mysql-zrm-reporter --fields backup-set,backup-status,backup-level,backup-date --type html --destination %s 2> /dev/null' % (bdir),
                                shell=True, stdout=subprocess.PIPE)
        doc = cmd.stdout.read()
        doc_soup = BeautifulSoup(doc)
        for element in doc_soup.findAll(attrs={'class': 'r_normal'}):
            bset_name = element.find(attrs={'class': 'c_backup_set'}).text
            if bset != bset_name:
                continue
            stat_string = element.find(attrs={'class': 'c_backup_status'}).text
            level = element.find(attrs={'class': 'c_backup_level'}).text
            date_string = element.find(attrs={'class': 'c_backup_date'}).text
            #Remove the day
            date_string = date_string.split(',', 1)[1]
            # Match strings like this <day> <month>, <year> <hr:min:sec> <AM/PM> (eg. 19 February, 2013 11:05:43 PM)
            p = re.compile('\d{2}\s\w+[,]\s\d{4}\s\d{2}[:]\d{2}[:]\d{2}\s\w{2}')
            m = p.search(date_string)
            
            if m is not None:
                dtime_backup = datetime.strptime(m.group(), '%d %B, %Y %I:%M:%S %p')
                
                if dtime_backup.timetuple() > (datetime.now() - timedelta(days=days_to_check)).timetuple():
                    #Backup ok
                    if 'Backup succeeded' in stat_string:
                        if level == '0':
                            lev0.append(0)
                        if level == '1':
                            lev1.append(0)
                    #Backup in progress
                    elif '----' in stat_string:
                        if level == '0':
                            lev0.append(1)
                        if level == '1':
                            lev1.append(1)
                    #Backup broken
                    else:
                        if level == '0':
                            lev0.append(2)
                        if level == '1':
                            lev1.append(2)
        return lev0, lev1    
    
if __name__ == "__main__":
    # Instantiate the plugin, check it, and then exit
    CheckZrmBackup().check().exit()
