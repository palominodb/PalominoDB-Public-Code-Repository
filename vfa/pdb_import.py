#!/usr/bin/python
# purpose perform a parallel import of databases.
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
import filecmp
import os
import sys
import subprocess
import time
from optparse import OptionParser
import grp, pwd
from pwd import getpwnam
from pdb_dba import *
#
# Addtional features
#   !!!! put in a safe guard so it doesn't run against a production db !!!!
#    o Confirm all pt tools are installed before running. This could save some headache
#    o Add exit codes with useful numbers(Work with Emanuel)
#    o Better logging/handling of failures
#    o Read user and pass from defaults-file
#    o Accept user and pass on command line
#    o The script should estimate completion time and show progress.
#    o Support views, triggers, stored proc, etc (low pri)
#    o Support running this script remotely.
#    o Have a force option so existing tables don't stop the script from running.
#    
# Import
#    o Modify my.cnf and restart db.


# Globals
# mysql_user="root"
# mysql_pass=""
unix_user = "mysql"

# Example : ./pdb_import.py --instance=localhost -b /tmp/dump  -p 3

# Some variables should be included as command line args when you have time.

def parse():
    parser = OptionParser(usage="usage: %prog -i [host:port] -b [backup_dir]",
                          version="%prog 0.1")
    parser.add_option("-i", "--instance",
                      action="store", # optional because it could be blank or use cluster instead
                      dest="inst",
                      default=False,
                      help="The instance to import.")
    parser.add_option("-b", "--backup-directory",
                      action="store", 
                      dest="backup_dir",
                      default=False,
                      help="The directory to import from.")
    parser.add_option("-d", "--defaults-file",
                      action="store",
                      dest="defaults_file",
                      default=False,
                      help="The file to read username and password from.")
    parser.add_option("-p", "--parallelism",
                      action="store", 
                      dest="parallelism",
                      default=2,
                      help="The number of import processes to run in parallel.")
    (options, args) = parser.parse_args()

    if options.inst == False:
        parser.error("Error: instance is required.")
    if options.backup_dir == False:
        parser.error("Error: backup directory must be given.")

    return (options, args)

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

#     search backup directory for directories
def get_db_list(dir):
    return os.walk(dir).next()[1] 

def create_databases(inst_host, inst_port, mysql_user, mysql_pass, db_list):
    for db in db_list: 
        result = run_select(inst_host, int(inst_port), mysql_user, mysql_pass, "create database if not exists " + db)

def get_table_file_list(backup_dir, db_list):
    table_file_list = []
    for db in db_list:
        file_list = os.listdir(backup_dir + "/" + db)
        for file_name in file_list:
            if re.match(".*\.sql$",file_name):
                table_file_list.append(backup_dir + "/" + db + "/" + file_name)

    return table_file_list

def create_tables(inst_host, inst_port, mysql_user, mysql_pass, table_file_list, backup_dir):
    socket = get_socket(inst_port)

    for table_file in table_file_list:
        db = table_file.split("/")[-2]
        table_name = table_file.split("/")[-1].split(".")[-2]

        cmd = "mysql --ignore --force -h " + inst_host + " -u " + mysql_user + " --socket=" + socket + " " + db + " < " + table_file
        # print cmd

        proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        return_code = proc.wait()

        if return_code > 0:
            for line in proc.stderr:
                print ("stderr: " + line.rstrip())
            sys.exit(1)

        for line in proc.stdout:
            print line.rstrip()
        for line in proc.stderr:
            print ("stderr: " + line.rstrip())

def checksum_tables(inst_host, inst_port, mysql_user, mysql_pass, table_list, backup_dir, parallelism):
    # Still need to figure out how to thread this and then write multiple threads to a file
    checksum_list = []

    for table in table_list:
        db = table.split(".")[0]
        table_name = table.split(".")[1]

        stmt = "checksum table " + db + "." + table_name
        result = run_select(inst_host, int(inst_port), mysql_user, mysql_pass, stmt)
        for row in result:
            checksum_list.append(row[0] + "," + str(row[1]))
            # print "DEBUG"
            # print row[0] + "." + str(row[1])

    checksum_list.sort()

    checksums_file = backup_dir + "/" + "checksums_at_import.txt"
    fo = open(checksums_file,'w')
    for checksum in checksum_list:
        fo.write(checksum + '\n')
    fo.close

def import_tables(inst_host, inst_port, mysql_user, mysql_pass, table_file_list, backup_dir, parallelism):
    socket = get_socket(inst_port)
    parallel_count = 0

    for table_file in table_file_list:
        db = table_file.split("/")[-2]
        table_name = table_file.split("/")[-1].split(".")[-2]

        ps = {}
        args = ['mysqlimport', '-h', inst_host,'-u', mysql_user, '--socket', socket, '--force','--ignore', db, backup_dir + "/" + db + "/" + table_name + ".txt"]
        p = subprocess.Popen(args)
        ps[p.pid] = p
        print ps

        parallel_count = parallel_count + 1

        if parallel_count >= parallelism:
            print "Waiting for %d processes..." % len(ps)
            while ps:
                pid, status = os.wait()
                if pid in ps:
                    del ps[pid]
                    print "Waiting for %d processes..." % len(ps)
                else:
                    parallel_count = 0

    # Do this until you get a better method of handling threads
    while ps:
        print "Waiting on last import processes to complete"
        pid, status = os.wait()
        if pid in ps:
            del ps[pid]
            print "Waiting for %d processes..." % len(ps)

# def set_grants
#     run the pt-grants file in backup dir

# def import_tables
#     import the smallest tables first?

def get_mysql_user_and_pass(options):

    if options.defaults_file:
        if os.path.exists(options.defaults_file):
            (mysql_user,mysql_pass) = get_mysql_user_and_pass_from_my_cnf(options.defaults_file)
    else:
        mysql_user='root'
        mysql_pass=''

    return (mysql_user, mysql_pass)

def get_table_list(table_file_list):
    table_list = []
    for table in table_file_list:
        (db,table_name) = table.split('.')[0].split('/')[-2:]
        table_list.append(db + "." + table_name)

    return table_list

def main():
    (options, args) = parse()
  
    inst = options.inst 
    (inst_host, inst_port) = parse_inst_info(inst)
    backup_dir = options.backup_dir
    parallelism = options.parallelism
    program_name_stripped = os.path.basename(__file__).split(".")[0]

    (mysql_user, mysql_pass) = get_mysql_user_and_pass(options)

    log_file = backup_dir + "/" + program_name_stripped + "_import.log"

    # If the directory does not exist error and exit
    if not os.path.exists(backup_dir):
        print "Error: %s does not exist." % backup_dir
        sys.exit(1)

    # check the start and end files

    if test_conn(mysql_user, mysql_pass, inst) == 1:
        print "Error: unable to connect to the instance. Check to confirm that it is available."
        sys.exit(1)

    # set read_only
    if set_read_only(host=inst_host, port=inst_port, user=mysql_user, password=mysql_pass) == 1:
        print "Instance has been set to read_only mode."
    else:
        print "Error: problem enabling read_only."
        sys.exit(1)

    # flush logs
    flush_logs(host=inst_host, port=inst_port, user=mysql_user, password=mysql_pass)

    # apply the pt-grants stored in the backup directory
    # set_grants

    # get list of databases    
    db_list = get_db_list(backup_dir)

    # create databases
    create_databases(inst_host, inst_port, mysql_user, mysql_pass, db_list)
    
    # get table files
    table_file_list = get_table_file_list(backup_dir, db_list)

    # create the empty tables
    create_tables(inst_host, inst_port, mysql_user, mysql_pass, table_file_list, backup_dir)

    # import_tables 
    import_tables(inst_host, inst_port, mysql_user, mysql_pass, table_file_list, backup_dir, parallelism)
  
    table_list = get_table_list(table_file_list)

    # checksum tables    
    checksum_tables(inst_host, inst_port, mysql_user, mysql_pass, table_list, backup_dir, parallelism)

# call main 
if __name__ == '__main__':
    main()

