#!/bin/bash
#
# Very basic Postgres information collection script.
# BSD License . PalominoDB team.
# Author: Emanuel Calvo
#
# TODO:
# - Add better output
# - Suggestions according the findings
# - Master/Slave detection and status
# - Summary 
# - Split the queries in a section/file to make this more extensible

VERSION="1.0b"

# You should not change variables here, please use the parameters.
LOG=review_$(hostname).log
DEF_PGUSER=postgres
PGUSER=$DEF_PGUSER
PGHOST=""
PORT=5432

usage()
{
cat << EOF
usage: $0 options

Postgres review script


OPTIONS:
    -h HOST                 Set the remote host. By default connects through socket.
    -o <file>               Output the report to a file Default: $LOG
    -c                      Set log check (non available remotely or using -h option)
    -H                      THIS
    -b <psql dir>           That is if you want to execute an specific psql command location (several versions?). Your actual psql is under: $(whereis psql || echo "You don't have it." ) 
    -u <POSTGRES USER>      The database user Default= $DEF_PGUSER
    -p <port>               Not implemented yet. Default: $PORT
    -V                      Version $VERSION
EOF
}

_line_()
{
  echo "" >> $LOG
  echo "#################################################################################################" >> $LOG
}

_section_()
{
  echo "### $1" | tee -a $LOG 
}


if [ command -v pg_config >/dev/null 2>&1 ]
then
  PGBINHOME=$(pg_config --bindir) 
else
  echo >&2 "We recommend to install pg_config utility" 
fi

while getopts h:o:cb:u:VH optname
  do
    case "$optname" in
      "h")
        PGHOST="-h $OPTARG" || { echo "Error setting host variable" ; exit 10 ; }
        ;;
      "o")
        LOG=$OPTARG
        ;;
      "c")
        LOG_CHECK=1
        ;;
      "b")
        test -e $OPTARG && PGBINHOME=$OPTARG || { echo "Some problem setting the bin folder" ; exit 10 ; }
        ;;
      "u")
        PGUSER="${OPTARG:=$DEF_PGUSER}" || { echo "Bad DB user" ; exit 12 ; }
        ;;
      "V")
        echo $VERSION
        exit 0
        ;;
      *|"H")
        usage
        exit 100
        ;;
    esac
# Mmmh... currently is not possible (but we can implement it)
[ ! "$PGHOST" == "" -a "$LOG_CHECK" == "1" ] && { echo "You have enabled the log checks, we don't support remotely check through ssh." ; exit 11 ; }
done


## Checks
##########
[ -e $LOG ] && cat /dev/null > $LOG

[ ! $PGBINHOME ] && { echo "No folder for postgres binaries. Please set the -b <folder> option" ; exit 15 ; }

PSQL="$PGBINHOME/psql"

[ "$(test -x $PSQL ; echo $?)" -ne "0" ] && { echo "pg_config bindir is not set correctly or binary folder doesn't have psql" ; exit 12 ; }

$PSQL -U $PGUSER $PGHOST template1 -c "select 0" > /dev/null || { echo "Failed to connect to postgres, code $?" ; exit 9 ; }

PG_LOGS=$($PSQL -U $PGUSER $PGHOST template1 -Atc "select string_agg(setting,'/') from pg_settings where name ~ 'log_directory|data_directory'")
PG_SIZE_ALL=$($PSQL -U $PGUSER $PGHOST template1 -Atc "select pg_size_pretty(sum(pg_database_size(datname))::bigint) from pg_database; ")



## Cluster Info
###############

_line_
_section_ "CLUSTER DETAILS AND INFORMATION"
_section_ "Size of all the DBs of the current cluster: $PG_SIZE_ALL"
_line_

_section_ "Configuration (only 1 output per cluster):" 

$PSQL -U $PGUSER $PGHOST template1 -c "\
      select regexp_replace(category,'(Previous PostgreSQL|Compatibility|Settings$|and Authentication$)','','g') ,\
             context, name || ' = ' || setting \
         from pg_settings \
        where  category !~ 'File Locations' order by category" >> $LOG

_line_
_section_ "File Locations and file related:" 

$PSQL -U $PGUSER $PGHOST template1 -c "select name, setting, context, category \
      from pg_settings where category ~ 'File Locations'" >> $LOG

_line_
_section_ "Is replication set up? This query will fail for < 9.0 versions."

$PSQL -U $PGUSER $PGHOST template1 -c "select count(*) OVER (), client_addr , client_hostname, state, sync_state from pg_stat_replication " >> $LOG


# Per DB Info collector
# Iterates through all the databases of the cluster
###################################################

for i in $($PSQL -U $PGUSER $PGHOST template1 -Atc "\
       select datname from pg_database\
       where datname !~ 'template0|template1|postgres' ")
do
  _line_
  _section_ " DATABASE  $i" 
  _line_

  _line_
  _section_ "Database stats:" 

  $PSQL -U $PGUSER $PGHOST $i -xc "\
      select *, (tup_returned+tup_fetched)/(tup_inserted+tup_updated+tup_deleted)\
       || ' to 1' as Ratio_R_W \
       from pg_stat_database where datname like '$i'" >> $LOG

  _line_
  _section_ "Activity in amounts: " 

  $PSQL -U $PGUSER $i $PGHOST -xc "select st.schemaname, st.relname, seq_scan , \
      seq_tup_read ,  idx_scan  , idx_tup_fetch , n_tup_ins , n_tup_upd , n_tup_del \
      ,pg_relation_size(st.schemaname || '.' || quote_ident(st.relname)) as size, \
      pg_size_pretty(pg_relation_size(st.schemaname || '.' || quote_ident(st.relname))) as pretty,heap_blks_read \
      , heap_blks_hit , idx_blks_read , idx_blks_hit , toast_blks_read , toast_blks_hit , \
      tidx_blks_read , tidx_blks_hit  \
      from pg_stat_user_tables st JOIN pg_statio_user_tables io USING (relid) \
      order by size desc limit 5"  >> $LOG

  _line_
  _section_ "Candidates to increase the STATISTICS target" 

   $PSQL -U $PGUSER $PGHOST $i -c "\
       select tablename, attname, n_distinct,(most_common_vals::text::text[])[1], \
              most_common_freqs[1]  \
       from pg_Stats  \
       where schemaname not in ('pg_catalog', 'information_schema')  \
         and n_distinct between 100 and 500 and most_common_freqs[1] < 0.18  \
      order by n_distinct desc" >> $LOG


  _line_
  _section_ "Dirty rows: " 

  $PSQL -U $PGUSER $i $PGHOST -c "select schemaname, relname, n_live_tup, n_dead_tup, \
       pg_size_pretty(pg_relation_size(schemaname || '.' || quote_ident(relname))) as size \
       from pg_stat_user_tables order by n_dead_tup desc limit 5"  >> $LOG
  
  _line_
  _section_ "Biggest 10 tables: " 

  $PSQL -U $PGUSER $i $PGHOST -c "select schemaname, relname, n_live_tup, \
       pg_size_pretty(pg_relation_size(schemaname || '.' || quote_ident(relname))) as size \
       from pg_stat_user_tables order by pg_relation_size(schemaname || '.' || quote_ident(relname)) desc \
       limit 10"  >> $LOG

  _line_
  _section_ "Inherited tables size: " 

  $PSQL -U $PGUSER $i $PGHOST -c "select inhparent::regclass, sum(pg_relation_size(inhrelid::regclass))::bigint, \
     pg_size_pretty(sum(pg_relation_size(inhrelid::regclass))::bigint) \
     from pg_inherits \
     group by 1 order by 2 desc" >> $LOG

  _line_
  _section_ "Dirty rows: " 

   $PSQL -U $PGUSER $i $PGHOST -c "select relname, n_live_tup, n_dead_tup, \
            pg_size_pretty(pg_relation_size(schemaname || '.' || relname)) \
            FROM pg_stat_user_tables \
            ORDER by n_dead_tup desc limit 10; \
            SELECT sum(n_live_tup) as Total_Live_rows, sum(n_dead_tup) as Total_Dead_Rows, \
            round(sum(n_dead_tup)*100/sum(n_live_tup),2) as Percentage_of_Dead_Rows, \
            pg_size_pretty(sum(pg_relation_size(schemaname || '.' || relname))::bigint) \
            FROM pg_stat_user_tables;" >> $LOG
  
  _line_
  _section_ "Update ratio - FILLFACTOR enhacements: " 
  
  $PSQL -U $PGUSER $i $PGHOST -c"\
  SELECT t.schemaname, t.relname, c.reloptions,\
       t.n_tup_upd, t.n_tup_hot_upd,\
       case when n_tup_upd > 0\
            then ((n_tup_hot_upd::numeric/n_tup_upd::numeric)*100.0)::numeric(5,2) \
            else NULL \
        end AS hot_ratio \
   FROM pg_stat_all_tables t \
      JOIN (pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid) \
        ON n.nspname = t.schemaname AND c.relname = t.relname \
   ORDER BY n_tup_upd desc LIMIT 20;" >> $LOG
   
   _line_ 
   _section_ "Duplicated indexes:" 
   
   $PSQL -U $PGUSER $i $PGHOST -c"\
   SELECT pg_size_pretty(sum(pg_relation_size(idx))::bigint) AS size, \
       (array_agg(idx))[1] AS idx1, (array_agg(idx))[2] AS idx2, \
       (array_agg(idx))[3] AS idx3, (array_agg(idx))[4] AS idx4 \
   FROM (\
    SELECT indexrelid::regclass AS idx, (indrelid::text ||E'\n'|| indclass::text ||E'\n'|| indkey::text ||E'\n'|| \
                                         coalesce(indexprs::text,'')||E'\n' || coalesce(indpred::text,'')) AS KEY \
     FROM pg_index) sub \
   GROUP BY KEY HAVING count(*)>1 \
   ORDER BY sum(pg_relation_size(idx)) DESC" >> $LOG
   
   _line_
   _section_ "Index Summary:" 

   $PSQL -U $PGUSER $i $PGHOST -c"\
   SELECT\
    pg_class.relname,\
    pg_size_pretty(pg_class.reltuples::bigint) AS rows_in_bytes,\
    pg_class.reltuples AS num_rows,\
    count(indexname) AS number_of_indexes,\
    CASE WHEN x.is_unique = 1 THEN 'Y'\
       ELSE 'N'\
    END AS UNIQUE,\
    SUM(case WHEN number_of_columns = 1 THEN 1\
              ELSE 0\
            END) AS single_column,\
    SUM(case WHEN number_of_columns IS NULL THEN 0\
             WHEN number_of_columns = 1 THEN 0\
             ELSE 1\
           END) AS multi_column\
    FROM pg_namespace \
       LEFT OUTER JOIN pg_class ON pg_namespace.oid = pg_class.relnamespace\
       LEFT OUTER JOIN\
       (SELECT indrelid,\
           max(CAST(indisunique AS integer)) AS is_unique\
       FROM pg_index\
       GROUP BY indrelid) x\
       ON pg_class.oid = x.indrelid\
   LEFT OUTER JOIN\
    ( SELECT c.relname AS ctablename, ipg.relname AS indexname, x.indnatts AS number_of_columns FROM pg_index x\
           JOIN pg_class c ON c.oid = x.indrelid\
           JOIN pg_class ipg ON ipg.oid = x.indexrelid  )\
    AS foo\
    ON pg_class.relname = foo.ctablename\
  WHERE \
     pg_namespace.nspname='public'\
  AND  pg_class.relkind = 'r'\
  GROUP BY pg_class.relname, pg_class.reltuples, x.is_unique\
  ORDER BY 2;" >> $LOG

  _line_
  _section_ "Index Statistics:" 

   $PSQL -U $PGUSER $i $PGHOST -c"SELECT\
    t.tablename,\
    indexname,\
    c.reltuples AS num_rows,\
    pg_size_pretty(pg_relation_size(quote_ident(t.tablename)::text)) AS table_size,\
    pg_size_pretty(pg_relation_size(quote_ident(indexrelname)::text)) AS index_size,\
    CASE WHEN x.is_unique = 1  THEN 'Y'\
       ELSE 'N'\
    END AS UNIQUE,\
    idx_scan AS number_of_scans,\
    idx_tup_read AS tuples_read,\
    idx_tup_fetch AS tuples_fetched\
  FROM pg_tables t\
   LEFT OUTER JOIN pg_class c ON t.tablename=c.relname\
   LEFT OUTER JOIN\
       (SELECT indrelid,\
           max(CAST(indisunique AS integer)) AS is_unique\
       FROM pg_index\
       GROUP BY indrelid) x\
       ON c.oid = x.indrelid\
   LEFT OUTER JOIN\
    ( SELECT c.relname AS ctablename, ipg.relname AS indexname, x.indnatts AS number_of_columns, \
     idx_scan, idx_tup_read, idx_tup_fetch,indexrelname FROM pg_index x\
           JOIN pg_class c ON c.oid = x.indrelid\
           JOIN pg_class ipg ON ipg.oid = x.indexrelid\
           JOIN pg_stat_all_indexes psai ON x.indexrelid = psai.indexrelid )\
    AS foo\
    ON t.tablename = foo.ctablename\
  WHERE t.schemaname='public'\
  ORDER BY 1,2; " >> $LOG


  _line_
  _section_ "Actual Table cache hit ratio"
  $PSQL -U $PGUSER $i $PGHOST -c" SELECT\
    'cache hit rate' AS name,\
     sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)) AS ratio \
     FROM pg_statio_user_tables; " >> $LOG

  _line_
  _section_ "Actual Index cache hit ratio"
  $PSQL -U $PGUSER $i $PGHOST -c" SELECT\
    'index hit rate' AS name,\
    (sum(idx_blks_hit)) / sum(idx_blks_hit + idx_blks_read) AS ratio\
    FROM pg_statio_user_indexes; " >>$LOG


done


## Log Collector Info
#####################

if [ $LOG_CHECK ]
then
  # The following lines could show error if logging_collector isn't enabled.
  _line_
  _section_ "Deadlocks:" 
  grep -c deadlock $PG_LOGS/* >> $LOG
  _line_
  _section_ "Timeouts:" 
  grep -c "canceling statement due to statement timeout" $PG_LOGS/* >> $LOG
fi 


## The end
##########
_line_
echo "All the information was dump to $LOG file."

exit 0

