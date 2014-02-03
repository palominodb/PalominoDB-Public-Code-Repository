## Postgres Graphs for Cacti 

### Files manifest: 

ss_get_postgres_stats.php
pg.cluster.ht.xml
pg.dbspecific.ht.xml

### To set up: 

Assumptions: Cacti installed and running. Set of postgres hosts running. The hosts are reachable from the cacti server by either ping or SNMP, and either directly via the postgres ports (usually 5432/5433), or reachable via ssh.

1. Import the two host templates. 

Import Templates --> Select Files.

    pg.cluster.ht.xml
    pg.dbspecific.ht.xml

If encryption is desired:
Generate keys for a the user that cacti runs as on the monitoring host. Select a user on each of the hosts to be monitored. Ensure that user can psql to the local database via psql.
(It doesn't have to be the same user on each host, but it is helpful as you can then put that user as the $pgsql_ssh_user and then you don't have to fill in that field when creating graphs). Put the public key for the monitoring host cacti user in the authorized_keys files for the selected user in the hosts to be monitored. 
You may also need to connect to the hosts as cactiuser once to get the monitoring host in the known_hosts file.
You also must either set $pgsql_ssh_user in ss_get_postgres_stats.php below or set ssh_user for each of the graphs that you want encrypted comms to the host. Most likely you'll want to set it in ss_get_postgres_stats.php.

2. copy ss_get_postgres_stats.php to the $CACTI_HOME/scripts directory

important defaults to adjust:

    $pgsql_user = 'postgres'; 
    $pgsql_pass = ''; 
    $pgsql_port = 5432; 
    $pgsql_db = 'postgres';
    $pgsql_ssh_user = ''; # if this is non-zero length, all requests will be ssh requests as this user (or can be overridden by changing the parameter for a specific graph)
Cacti variables:

    $debug: makes output more verbose
    $debug_log: if set it's where the verbose output will go to. Take note of this to either rotate it or turn off debugging so it doesn't get filled up.

3. Create new devices. You should create one device for each server you want to monitor, and then create a separate device for each separate database in each cluster you want to monitor.
Choose a host template, either "PDB Postgres Server Cluster HT" or "PDB Postgres Server DB-Specific HT".

##### Create graphs:

Devices-> select your device -> Create Graphs For This Host. You can probably select all the graphs that show up. Note that not creating a graph will not change what parameters are retrieved.
To create your graphs, for each of the fields, they will default to the values given above in your ss_get_postgres_stats.php script, so it will save you time in this step if your defaults were chosen efficiently up above.

Wait for your graphs to show up.

### Common problems: 

If the script is connecting for the first time as the cacti user to a given host, it will prompt as a new host. Therefore you should connect manually or other method of getting the key into the known_hosts file on the target hosts.

Ensure the key is set up for the correct users: 

monitoring host: the user that cacti runs at should have a key pair generated.

target hosts: the user that is set in "$pgsql_ssh_user" must have the above public key in its authorized_keys file.

If postgres is running on a specific host using a port that is different from the $pgsql_port given above, you still have to specify that port for each of the graphs in that host, even if you are connecting via ssh. (It still connects via that port only locally)
If you have a db-specific graph set up, ensure the database name is correct. If none is given, it will use the default database given in $pgsql_db.

