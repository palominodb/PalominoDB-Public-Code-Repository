#!/usr/bin/python
# Filename: pdb_dba.py
# Purpose : a module for various functions we perform
#
# Tip: use the following when you call this module so that you don't
#      have to include the module name as well as the function
#
#      from pdb_dba import *
#
#   Copyright 2013 Palominodb
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

import os
import re
import subprocess
import sys
import warnings 
import MySQLdb
warnings.simplefilter("error", MySQLdb.Warning)
# http://python.6.n6.nabble.com/Trapping-warnings-from-MySQLdb-td1735661.html
from os.path import expanduser
from ConfigParser import SafeConfigParser

vfa_cnf_dir="/etc"

def local_exec(cmd):
    # Probably should return the status of the script that was run
    proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    return_code = proc.wait()

    for line in proc.stdout:
        print (line.rstrip())
    for line in proc.stderr:
        print (line.rstrip())

    return return_code

def is_mysqld_running(port=3306, silent=False):
    # This should be just the start of what to check. Checking connectivity
    # would be something else to check as well. Port info isn't available via
    # ps consistenly so I'm not sure how useful that is anymore.

    cmd = "lsof -i4 -P | grep -i mysql|grep \":" + str(port) + " \" |grep LISTEN | wc -l"
    proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    return_code = proc.wait()

    for line in proc.stdout:
        if silent == False:
            print int(line.rstrip())
        return int(line.rstrip())

def test_conn(user, password, inst='localhost:3306'):
    try:
        result = inst.index(':')
    except:
        result = 0        

    if result > 0: 
      inst_host=inst.split(':')[0]
      inst_port=inst.split(':')[1]
    else:
      inst_host=inst
      inst_port='3306'

    # Will need to have conditional for remote instances
    try:
        print "MySQLdb.connect(host=inst_host, unix_socket=" + get_socket(int(inst_port)) + ", user=" + user + ", passwd=password, db='')"
        MySQLdb.connect(host=inst_host, unix_socket=get_socket(int(inst_port)), user=user, passwd=password, db='')
        return 0
    except MySQLdb.Error, e:
        return 1

# A replacement for conn_db    
def conn(host, port, user, password, db):
    try:
        if re.match('^localhost', host):
            return MySQLdb.connect(host=host, unix_socket=get_socket(int(port)), user=user, passwd=password, db='')
        else:
            return MySQLdb.connect(host=host, port=int(port), user=user, passwd=password, db='')

    except MySQLdb.Error, e:
        sys.stderr.write("[ERROR] %d: %s\n" % (e.args[0], e.args[1]))
        return False

def conn_db(host, port, user, password, db):
    try:
        return MySQLdb.connect(host=host, unix_socket=get_socket(int(port)), user=user, passwd=password, db='')
    except MySQLdb.Error, e:
        sys.stderr.write("[ERROR] %d: %s\n" % (e.args[0], e.args[1]))
        return False

def run_select(host='localhost', port=3306, user='root', password='', stmt=''):
    # Returns a result set from the select.
    # Example use
    # result = run_select(stmt='select 1')
    # for row in result:
    #     print row[0]
    #

    db = conn(host, int(port), user, password, 'mysql')
    # dturner
    # conn = conn_db(host, int(port), user, password, 'mysql')

    cursor = db.cursor()  
   
    try:
        cursor.execute(stmt)
    except MySQLdb.Warning, e:
        # For now return nothing if there is a warning.
        return 

    result = cursor.fetchall()

    cursor.close
    db.close

    return result

def show_slave_status(host='localhost', port=3306, user='root', password=''):
    cmd = "show slave status"
    result = run_select(host,port,user,password,cmd)
    return result

def show_master_status(host='localhost', port=3306, user='root', password=''):
    # example usage
    # for row in result:
    #     print "%s,%s,%s,%s" % (row[0],row[1],row[2],row[3])
    #
    cmd = "show master status"
    result = run_select(host,port,user,password,cmd)
    return result

def stop_slave(host='localhost', port=3306, user='root', password=''):
    # todo:
    #      o add a check to confirm io_thread and sql_thread have been stopped
    #        or that replication hasn't been setup. 
    #      o find out how to timeout when the stop slave command hangs.
    #
    cmd = "stop slave"
    result = run_select(host,port,user,password,cmd)

    if not result:
        return 1
    # DEBUG - this needs to be tested with a db that's slaving.
    return result

def set_read_only(host='localhost', port=3306, user='root', password='', setting=1):
    cmd = "set global read_only=" + str(setting)
    result = run_select(host,port,user,password,cmd)
    cmd = "show global variables like 'read_only'"
    result = run_select(host,port,user,password,cmd)

    for row in result:
       if row[1] == "ON":
           return 1
       else:
           return 0

def set_variable(variable, value, host='localhost', port=3306, user='root', password=''):
    cmd = "set global " + variable + "=" + str(value)
    result = run_select(host,port,user,password,cmd)
    cmd = "show global variables like '" + variable + "'"
    result = run_select(host,port,user,password,cmd)

    for row in result:
           return row[1]

def flush_logs(host='localhost', port=3306, user='root', password=''):
    cmd = "flush logs"
    result = run_select(host,port,user,password,cmd)
    

def get_vfa_cnf_file():

    home = expanduser("~")
    if os.path.isfile( home + "/vfatab" ):
        vfa_cnf_file = home + "/vfatab"
    elif os.path.isfile( "/tmp/vfatab" ):
        vfa_cnf_file = "/tmp/vfatab"
    elif os.path.isfile( "/etc/vfatab" ):
        vfa_cnf_file = "/etc/vfatab"
    else:
        print "Error: /etc/vfatab has not been configured"
        return 1
        
    return vfa_cnf_file

def get_my_cnf_file(port=3306):
    vfa_cnf_file = get_vfa_cnf_file()

    try:
        fr = open(vfa_cnf_file, "r")
    except IOError:
        # I do not think it should return and error. It
        # should just return nothing for the file.
        return

    while 1:
        line = fr.readline()
        if not line:
            break
        if re.match('^.*:' + str(port) + ':',line):
            return line.split(":")[0]
    return 

def get_my_cnf_parm(port=3306, section="mysqld",var=""):

    my_cnf_file = get_my_cnf_file(port)

    cmd='egrep -i "\[' + section + '\]|^' + var + '" ' + my_cnf_file + ' | grep -A1 "\[' + section +'\]"|tail -1' + " | awk -F'=' \'{print $2}\' |sed 's/ //g'"
    proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    return_code = proc.wait()

    for line in proc.stdout:
        return line.rstrip()

    return

def get_socket(port):
    
    my_cnf_file = get_my_cnf_file(port)

    cmd='egrep -i "\[mysqld\]|socket" ' + my_cnf_file + ' | grep -A1 "\[mysqld\]"|tail -1' + " | awk -F'=' \'{print $2}\' |sed 's/ //g'" 
    # cmd='egrep -i "mysqld|socket" /etc/mysql/my-m3306.cnf |grep -A1 "\[mysqld\]"|tail -1 |awk -F"=" ''{print $2}'''
    proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    return_code = proc.wait()

    for line in proc.stdout:
        return line.rstrip()

    return

def get_mysql_user_and_pass_from_my_cnf(my_cnf_file):

    try:
        fr = open(my_cnf_file, "r")
    except IOError:
        # I do not think it should return and error. It
        # should just return nothing for the password.
        return

    while 1:
        line = fr.readline()
        if not line:
            break
        line = line.strip().replace('"','')
        if re.match("^user",line):
            mysql_user = line.split("=")[1]
        if re.match("^password",line):
            mysql_pass = line.split("=")[1]

    return (mysql_user,mysql_pass)


# take what could be non standard instance definitiion
# and return 3 cleanly formated variables ready for consumption
def return_inst_info(inst=False):
    inst = inst.lower()

    if inst == False:
        inst = 'localhost:3306'
        inst_host = 'localhost'
        inst_port = 3306

        return (inst, inst_host, inst_port)

    inst_parsed = inst.split(":")
    inst_parsed_len = len(inst_parsed)

    if inst_parsed_len < 3:
        if inst_parsed_len < 2:
            inst_host=inst_parsed[0]
            inst_port=3306
            inst="%s:%d" % (inst_host, inst_port)
        else:
            inst_host=inst_parsed[0]
            inst_port=int(inst_parsed[1])
            inst="%s:%d" % (inst_host, inst_port)
    else:
        # DEBUG raise exception here.
        print "Error: parsing problem with inst."
        return

    return (inst, inst_host, inst_port)


def parse_inst_info(inst):
    try:
        result = inst.index(':')
    except:
        result = 0

    if result > 0:
      inst_host=inst.split(':')[0]
      inst_port=inst.split(':')[1]
    else:
      inst_host=inst
      inst_port='3306'

    return (inst_host, inst_port)

def is_production_server(server):
    pass

def is_production_instance(inst):
    pass




