<?php

# ============================================================================
# This is a script to retrieve information from a PostgreSQL server for input to a
# Cacti graphing process, based on Percona's ss_get_mysql_stats.php
# 
# License: GPL License (see COPYING)
# Copyright 2013 PalominoDB
# Authors:
#  Moss Gross
# ============================================================================

# ============================================================================
# To make this code testable, we need to prevent code from running when it is
# included from the test script.  The test script and this file have different
# filenames, so we can compare them.  In some cases $_SERVER['SCRIPT_FILENAME']
# seems not to be defined, so we skip the check -- this check should certainly
# pass in the test environment.
# ============================================================================
if ( !array_key_exists('SCRIPT_FILENAME', $_SERVER)
   || basename(__FILE__) == basename($_SERVER['SCRIPT_FILENAME']) ) {

# ============================================================================
# CONFIGURATION
# ============================================================================
# Define connection constants in config.php.  Arguments explicitly passed
# in from Cacti will override these.  However, if you leave them blank in Cacti
# and set them here, you can make life easier.  Instead of defining parameters
# here, you can define them in another file named the same as this file, with a
# .cnf extension.
# ============================================================================
# the following can be overridden by per-graph parameters
$pgsql_user = 'postgres'; 
$pgsql_pass = ''; 
$pgsql_port = 5432; 
#$pgsql_ssh_user = 'ec2-user'; # if this is non-zero length, all requests will be ssh requests as this user (or can be overridden by changing the parameter for a specific graph)
$pgsql_ssh_user = ''; # if this is non-zero length, all requests will be ssh requests as this user (or can be overridden by changing the parameter for a specific graph)
$pgsql_db   = 'postgres'; 

# The following is not overridden by parameters
$default_slow_query_seconds = 30;

$cache_dir  = '/tmp';  # If set, this uses caching to avoid multiple calls.
$poll_time  = 300;     # Adjust to match your polling interval.
$timezone   = null;    # If not set, uses the system default.  Example: "UTC"
$chk_options = array (
);

$use_ss    = FALSE; # Whether to use the script server or not
$debug     = FALSE; # Define whether you want debugging behavior.
$debug_log = ""; # If $debug_log is a filename, it'll be used.
#$debug_log = "/tmp/ss_get_postgres_stats.log"; # If $debug_log is a filename, it'll be used.

# ============================================================================
# You should not need to change anything below this line.
# ============================================================================
$version = '1.0.1';

# ============================================================================
# Include settings from an external config file (issue 39).
# ============================================================================
if ( file_exists(__FILE__ . '.cnf' ) ) {
   debug("Found configuration file " . __FILE__ . ".cnf");
   require(__FILE__ . '.cnf');
}

# Make this a happy little script even when there are errors.
$no_http_headers = true;
ini_set('implicit_flush', false); # No output, ever.
if ( $debug ) {
   ini_set('display_errors', true);
   ini_set('display_startup_errors', true);
   ini_set('error_reporting', 2147483647);
}
else {
   ini_set('error_reporting', E_ERROR);
}
//ob_start(); # Catch all output such as notices of undefined array indexes.
function error_handler($errno, $errstr, $errfile, $errline) {
   print("$errstr at $errfile line $errline\n");
   debug("$errstr at $errfile line $errline");
}
# ============================================================================
# Set up the stuff we need to be called by the script server.
# ============================================================================
if ( $use_ss ) {
   if ( file_exists( dirname(__FILE__) . "/../include/global.php") ) {
      # See issue 5 for the reasoning behind this.
      debug("including " . dirname(__FILE__) . "/../include/global.php");
      include_once(dirname(__FILE__) . "/../include/global.php");
   }
   elseif ( file_exists( dirname(__FILE__) . "/../include/config.php" ) ) {
      # Some Cacti installations don't have global.php.
      debug("including " . dirname(__FILE__) . "/../include/config.php");
      include_once(dirname(__FILE__) . "/../include/config.php");
   }
}

# ============================================================================
# Set the default timezone either to the configured, system timezone, or the
# default set above in the script.
# ============================================================================
if ( function_exists("date_default_timezone_set")
   && function_exists("date_default_timezone_get") ) {
   $tz = ($timezone ? $timezone : @date_default_timezone_get());
   if ( $tz ) {
      @date_default_timezone_set($tz);
   }
}

# ============================================================================
# Make sure we can also be called as a script.
# ============================================================================
if (!isset($called_by_script_server)) {
   debug($_SERVER["argv"]);
   array_shift($_SERVER["argv"]); # Strip off this script's filename
   $options = parse_cmdline($_SERVER["argv"]);
   validate_options($options);
   $result = ss_get_postgres_stats($options);
   debug($result);
   if ( !$debug ) {
      # Throw away the buffer, which ought to contain only errors.
      //ob_end_clean();
   }
   else {
      //ob_end_flush(); # In debugging mode, print out the errors.
   }

   # Split the result up and extract only the desired parts of it.
   $wanted = explode(',', $options['items']);
   $output = array();
   foreach ( explode(' ', $result) as $item ) {
      if ( in_array(substr($item, 0, 2), $wanted) ) {
         $output[] = $item;
      }
   }
   debug(array("Final result", $output));
   print(implode(' ', $output));
}

# ============================================================================
# End "if file was not included" section.
# ============================================================================
}

# ============================================================================
# Work around the lack of array_change_key_case in older PHP.
# ============================================================================
if ( !function_exists('array_change_key_case') ) {
   function array_change_key_case($arr) {
      $res = array();
      foreach ( $arr as $key => $val ) {
         $res[strtolower($key)] = $val;
      }
      return $res;
   }
}

# ============================================================================
# Validate that the command-line options are here and correct
# ============================================================================
function validate_options($options) {
   debug($options);
   $opts = array('host', 'items', 'user', 'pass', 'nocache', 'port', 'db', 'ssh_user', 'db_specific');
   # Required command-line options
   foreach ( array('host', 'items') as $option ) {
      if ( !isset($options[$option]) || !$options[$option] ) {
         usage("Required option --$option is missing");
      }
   }
   foreach ( $options as $key => $val ) {
      if ( !in_array($key, $opts) ) {
         usage("Unknown option --$key");
      }
   }
}

# ============================================================================
# Print out a brief usage summary
# ============================================================================
function usage($message) {
   global $pgsql_user, $pgsql_pass, $pgsql_port, $pgsql_db, $pgsql_ssh_user;

   $usage = <<<EOF
$message
Usage: php ss_get_postgres_stats.php --host <host> --items <item,...> [OPTION]

   --host        Hostname to connect to; use host:port syntax to specify a port
                 Use :/path/to/socket if you want to connect via a UNIX socket
   --items       Comma-separated list of the items whose data you want
   --user        PostgreSql username; defaults to $pgsql_user if not given
   --pass        PostgreSql password; defaults to $pgsql_pass if not given
   --db          PostgreSql database; defaults to $pgsql_db if not given
   --db_specific Only for database-specific queries 
   --ssh_user    If given, will use ssh as this user to connect to hosts - pub key must exist; 
                 (note this is not the same as using specially-compiled ssh-enabled postgres)
   --nocache     Do not cache results in a file
   --port        PostgreSql port; defaults to $pgsql_port if not given

EOF;
   die($usage);
}

# ============================================================================
# Parse command-line arguments, in the format --arg value --arg value, and
# return them as an array ( arg => value )
# ============================================================================
function parse_cmdline( $args ) {
   $result = array();
   $cur_arg = '';
   foreach ($args as $val) {
      if ($val == '--db_specific') {
        $result['db_specific'] = 1;
        $cur_arg = '';
        continue;
      }
      if ( strpos($val, '--') === 0 ) {
         if ( strpos($val, '--no') === 0 ) {
            # It's an option without an argument, but it's a --nosomething so
            # it's OK.
            $result[substr($val, 2)] = 1;
            $cur_arg = '';
         }
         elseif ( $cur_arg ) { # Maybe the last --arg was an option with no arg
            if ( $cur_arg == '--user' || $cur_arg == '--pass' || $cur_arg == '--port' || $cur_arg == '--db' || $cur_arg == '--ssh_user') {
               # Special case because Cacti will pass these without an arg
               # has the effect of shifting over one
               $cur_arg = $val;
            }
            else {
               die("No arg: $cur_arg\n");
            }
         }
         else {
            $cur_arg = $val;
         }
      }
      else {
         $result[substr($cur_arg, 2)] = $val;
         $cur_arg = '';
      }
   }
   if ( $cur_arg && ($cur_arg != '--user' && $cur_arg != '--pass' && $cur_arg != '--port' && $cur_arg != '--db' && $cur_arg != '--ssh_user') ) {
      die("No arg: $cur_arg\n");
   }
   debug($result);
   return $result;
}

# ============================================================================
# This is the main function.  Some parameters are filled in from defaults at the
# top of this file.
# ============================================================================
function ss_get_postgres_stats( $options ) {
   # Process connection options and connect to PostgreSQL.
   global $debug, $pgsql_user, $pgsql_pass, $cache_dir, $poll_time,
          $chk_options, $pgsql_port, $mysql_ssl, $pgsql_db, 
          $pgsql_ssh_user, $user, $pass, $db, $host, $ssh_user,
          $port, $default_slow_query_seconds;

   # Connect to PostgreSQL.
   $user = isset($options['user']) ? $options['user'] : $pgsql_user;
   $pass = isset($options['pass']) ? $options['pass'] : $pgsql_pass;
   $port = isset($options['port']) ? $options['port'] : $pgsql_port;
   $ssh_user = isset($options['ssh_user']) && strlen(trim($options['ssh_user'])) > 0 ? $options['ssh_user'] : $pgsql_ssh_user;
   $db = isset($options['db']) ? $options['db'] : $pgsql_db;
   $db_specific = isset($options['db_specific']) ? true : false;
   $host = $options['host'];
   if ($ssh_user) {
     $conn = null;
   } else {
     $host_str  = $options['host'];
     debug(array('connecting to', $host_str, $user, $pass));
     if ( !extension_loaded('pgsql') ) {
        debug("The PostgreSQL extension is not loaded");
        die("The PostgreSQL extension is not loaded");
     }
     $conn = pg_connect("host=$host_str dbname=$db user=$user password=$pass port=$port");
     if ( !$conn ) {
        debug("PostgreSQL connection failed: " . pg_last_error());
        die("PostgreSQL: " . mysql_error());
     }
  }

   $sanitized_host
       = str_replace(array(":", "/"), array("", "_"), $options['host']);
   if (isset($options['db'])) {
     $cache_file = "$cache_dir/$sanitized_host-".$options['db']."-pgsql_stats";
   } else {
     $cache_file = "$cache_dir/$sanitized_host-pgsql_stats";
   }
   $cache_file .= (isset($options['port']) || $port != 5432 ? ":$port" : '');
   $cache_file .= ".txt";
   debug("Cache file is $cache_file");

   # First, check the cache.
   $fp = null;
   if ( !isset($options['nocache']) ) {
      if ( $fp = fopen($cache_file, 'a+') ) {
         $locked = flock($fp, 1); # LOCK_SH
         if ( $locked ) {
            if ( filesize($cache_file) > 0
               && filectime($cache_file) + ($poll_time/2) > time()
               && ($arr = file($cache_file))
            ) {# The cache file is good to use.
               debug("Using the cache file");
               fclose($fp);
               return $arr[0];
            }
            else {
               debug("The cache file seems too small or stale");
               # Escalate the lock to exclusive, so we can write to it.
               if ( flock($fp, 2) ) { # LOCK_EX
                  # We might have blocked while waiting for that LOCK_EX, and
                  # another process ran and updated it.  Let's see if we can just
                  # return the data now:
                  if ( filesize($cache_file) > 0
                     && filectime($cache_file) + ($poll_time/2) > time()
                     && ($arr = file($cache_file))
                  ) {# The cache file is good to use.
                     debug("Using the cache file");
                     fclose($fp);
                     return $arr[0];
                  }
                  ftruncate($fp, 0); # Now it's ready for writing later.
               }
            }
         }
         else {
            debug("Couldn't lock the cache file, ignoring it.");
            $fp = null;
         }
      }
   }
   else {
      $fp = null;
      debug("Couldn't open the cache file");
   }

   # Set up variables.
   $status = array( # Holds the result of SHOW STATUS, SHOW INNODB STATUS, etc
      # Define some indexes so they don't cause errors with += operations.
   );

   # make version-aware, versions > 9.1 require "query" vice "current_query"
   # I think this query actually fails for very low version, but the result will be correct
   $version_gt_91 = false;
   $version_query = "select translate(regexp_matches(version(),'\d\.\d')::text,'.{}','')::int > 91 as version_gt_91";
   $result = run_query($version_query, $conn);
   foreach ( $result as $row ) {
     if ($row['version_gt_91'] == "t") {
       $version_gt_91 = true;
     }
   }

   $query_table_name = ($version_gt_91 ? "query" : "current_query") ;
  

      if ($db_specific) {
      # per-database metrics - require specific database parameter to connect
      $queries = array(
      "select pg_database_size(current_database())/1024/1024/1024.0 db_size_gb",
      "select count(*) as db_locks from pg_locks where locktype != 'virtualxid' and database is not null;",
      "select sum(heap_blks_read) sum_heap_blks_read, sum(heap_blks_hit) sum_heap_blks_hit from pg_statio_user_tables",
       "select sum(idx_blks_read) sum_idx_blks_read, sum(idx_blks_hit) sum_idx_blks_hit from pg_statio_user_tables",
       "select sum(toast_blks_read) sum_toast_blks_read, sum(toast_blks_hit) sum_toast_blks_hit from pg_statio_user_tables",
       "select sum(confl_tablespace) ts_conflicts, sum(confl_lock) lock_conflicts, sum(confl_snapshot) snapshot_conflicts, sum(confl_bufferpin) bufferpin_conflicts, sum(confl_deadlock) deadlock_conflicts from pg_stat_database_conflicts",

      );

    array_push($queries, "SELECT
sum(ROUND(CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages/otta::numeric END,1) )AS tbloat
FROM (
  SELECT
    schemaname, tablename, cc.reltuples, cc.relpages, bs,
    CEIL((cc.reltuples*((datahdr+ma-
      (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta,
    COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
    COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
  FROM (
    SELECT
      ma,bs,schemaname,tablename,
      (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
      (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
    FROM (
      SELECT
        schemaname, tablename, hdr, ma, bs,
        SUM((1-null_frac)*avg_width) AS datawidth,
        MAX(null_frac) AS maxfracsum,
        hdr+(
          SELECT 1+count(*)/8
          FROM pg_stats s2
          WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
        ) AS nullhdr
      FROM pg_stats s, (
        SELECT
          (SELECT current_setting('block_size')::numeric) AS bs,
          CASE WHEN substring(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
          CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
        FROM (SELECT version() AS v) AS foo
      ) AS constants
      GROUP BY 1,2,3,4,5
    ) AS foo
  ) AS rs
  JOIN pg_class cc ON cc.relname = rs.tablename
  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND (nn.nspname NOT IN ('information_schema', 'pg_catalog'))
  LEFT JOIN pg_index i ON indrelid = cc.oid
  LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
) AS sml");
   array_push($queries, "SELECT
sum(ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) ) AS ibloat
FROM (
  SELECT
    schemaname, tablename, cc.reltuples, cc.relpages, bs,
    CEIL((cc.reltuples*((datahdr+ma-
      (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta,
    COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
    COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
  FROM (
    SELECT
      ma,bs,schemaname,tablename,
      (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
      (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
    FROM (
      SELECT
        schemaname, tablename, hdr, ma, bs,
        SUM((1-null_frac)*avg_width) AS datawidth,
        MAX(null_frac) AS maxfracsum,
        hdr+(
          SELECT 1+count(*)/8
          FROM pg_stats s2
          WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
        ) AS nullhdr
      FROM pg_stats s, (
        SELECT
          (SELECT current_setting('block_size')::numeric) AS bs,
          CASE WHEN substring(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
          CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
        FROM (SELECT version() AS v) AS foo
      ) AS constants
      GROUP BY 1,2,3,4,5
    ) AS foo
  ) AS rs
  JOIN pg_class cc ON cc.relname = rs.tablename
  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND (nn.nspname NOT IN ('information_schema', 'pg_catalog'))
  LEFT JOIN pg_index i ON indrelid = cc.oid
  LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
) AS sml");
   } else {
   $queries = array(
      "select sum(cluster.numbackends) as actual_backends, (select setting from pg_settings where name = 'max_connections') as max_connections from pg_stat_database cluster",
      "select count(*) idle_txn_backends from pg_stat_activity where $query_table_name ~ '<IDLE>*transaction'",
      "select count(*) slow_queries from pg_stat_activity where clock_timestamp() - query_start > ((select CASE WHEN setting::bigint < 0 THEN $default_slow_query_seconds WHEN  setting::bigint > 1000 THEN setting::bigint/1000 ELSE 1 END from pg_settings where name = 'log_min_duration_statement')::text || ' seconds')::interval and $query_table_name != '<IDLE>'",
      "select round(sum(pg_database_size(datname))/1024/1024/1024.0) all_dbs_size_gb from pg_database",
      "select sum(xact_commit) as commits, sum(xact_rollback) as rollbacks from pg_stat_database",
      "select sum(blks_read) as blocks_read, sum(blks_hit) as blocks_hit from pg_stat_database",
      "select sum(tup_returned) as returned_tuples, sum(tup_fetched) as fetched_tuples from pg_stat_database",
      "select sum(tup_inserted) as inserts, sum(tup_updated) as updates, sum(tup_deleted) as deletes from pg_stat_database",
      "select sum(confl_tablespace) sum_conf_tablespace, sum(confl_lock) sum_conf_lock, sum(confl_snapshot) sum_conf_snapshot, sum(confl_bufferpin) sum_conf_bufferpin, sum(confl_deadlock) sum_conf_deadlock from pg_stat_database_conflicts",
      "select checkpoints_timed,checkpoints_req,checkpoint_write_time,checkpoint_sync_time from pg_stat_bgwriter",
      "select buffers_checkpoint,buffers_clean, buffers_backend,buffers_backend_fsync from pg_stat_bgwriter",
      "SELECT ( extract('epoch' from now()) - extract('epoch' from pg_last_xact_replay_timestamp())) AS slave_lag", );
    }
   foreach ($queries as $query) {
     $result = run_query($query, $conn);

     foreach ( $result as $row ) {
        foreach ( $row as $key => $value) {
          $status["PGSQ_".$key] = $value;
        }
     }
   }


   # The following queries for locks are both for cluster and db-specific
   $query = "select mode, count(*) from pg_locks where locktype != 'virtualxid' group by mode";
   $result = run_query($query, $conn);
   foreach ($result as $row) {
       $mode = str_replace("exclusive", "exc", $row['mode']); // additional shortening rule to allow subsequent shortening to work
       $status["PGSQ_".camelcase_to_underscore($mode)] = $row['count'];
   }

   $query = "select locktype, count(*) from pg_locks group by locktype";
   $result = run_query($query, $conn);
   foreach ($result as $row) {
       $status["PGSQ_".$row['locktype']] = $row['count'];
   }

   # Define the variables to output.  I use shortened variable names so maybe
   # it'll all fit in 1024 bytes for Cactid and Spine's benefit.  Strings must
   # have some non-hex characters (non a-f0-9) to avoid a Cacti bug.  This list
   # must come right after the word MAGIC_VARS_DEFINITIONS.  The Perl script
   # parses it and uses it as a Perl variable.
   $keys = array(
      'PGSQ_actual_backends'           =>  'g0',
      'PGSQ_max_connections'           =>  'g1',
      'PGSQ_access_share_lock'         =>  'g2',
      'PGSQ_row_share_lock'            =>  'g3',
      'PGSQ_row_exc_lock'              =>  'g4',
      'PGSQ_share_update_exc_lock'     =>  'g5',
      'PGSQ_share_lock'                =>  'g6',
      'PGSQ_share_row_exc_lock'        =>  'g7',
      'PGSQ_exc_lock'                  =>  'g8',
      'PGSQ_access_exc_lock'           =>  'g9',
      'PGSQ_relation'                  =>  'ga',
      'PGSQ_extend'                    =>  'gb',
      'PGSQ_page'                      =>  'gc',
      'PGSQ_tuple'                     =>  'gd',
      'PGSQ_transactionid'             =>  'ge',
      'PGSQ_object'                    =>  'gf',
      'PGSQ_userlock'                  =>  'gg',
      'PGSQ_advisory'                  =>  'gh',
      'PGSQ_idle_txn_backends'         =>  'gi',
      'PGSQ_slow_queries'              =>  'gj',
      'PGSQ_all_dbs_size_gb'           =>  'gk',
      'PGSQ_commits'                   =>  'gl',
      'PGSQ_rollbacks'                 =>  'gm',
      'PGSQ_blocks_read'               =>  'gn',
      'PGSQ_blocks_hit'                =>  'go',
      'PGSQ_returned_tuples'           =>  'gp',
      'PGSQ_fetched_tuples'            =>  'gq',
      'PGSQ_inserts'                   =>  'gr',
      'PGSQ_updates'                   =>  'gs',
      'PGSQ_deletes'                   =>  'gt',
      'PGSQ_sum_conf_tablespace'       =>  'gu',
      'PGSQ_sum_conf_lock'             =>  'gv',
      'PGSQ_sum_conf_snapshot'         =>  'gw',
      'PGSQ_sum_conf_bufferpin'        =>  'gx',
      'PGSQ_sum_conf_deadlock'         =>  'gy',
      'PGSQ_checkpoints_timed'         =>  'gz',
      'PGSQ_checkpoints_req'           =>  'h0',
      'PGSQ_checkpoints_write_time'    =>  'h1',
      'PGSQ_checkpoints_sync_time'     =>  'h2',
      'PGSQ_buffers_checkpoint'        =>  'h3',
      'PGSQ_buffers_clean'             =>  'h4',
      'PGSQ_buffers_backend'           =>  'h5',
      'PGSQ_buffers_backend_fsync'     =>  'h6',
      'PGSQ_slave_lag'                 =>  'h7',
      'PGSQ_db_size_gb'                =>  'h8',
      'PGSQ_db_locks'                  =>  'h9',
      'PGSQ_sum_heap_blks_read'        =>  'ha',
      'PGSQ_sum_heap_blks_hit'         =>  'hb',
      'PGSQ_sum_idx_blks_read'         =>  'hc',
      'PGSQ_sum_idx_blks_hit'          =>  'hd',
      'PGSQ_sum_toast_blks_read'       =>  'he',
      'PGSQ_sum_toast_blks_hit'        =>  'hf',
      'PGSQ_tbloat'                    =>  'hg',
      'PGSQ_ibloat'                    =>  'hh',
      'PGSQ_ts_conflicts'              =>  'hi',
      'PGSQ_lock_conflicts'            =>  'hj',
      'PGSQ_snapshot_conflicts'        =>  'hk',
      'PGSQ_bufferpin_conflicts'       =>  'hl',
      'PGSQ_deadlock_conflicts'        =>  'hm',
   );

   # Return the output.
   $zero_out_no_responses = array (
      'PGSQ_access_share_lock',
      'PGSQ_row_share_lock',
      'PGSQ_row_exc_lock',
      'PGSQ_share_update_exc_lock',
      'PGSQ_share_lock',
      'PGSQ_share_row_exc_lock',
      'PGSQ_exc_lock',
      'PGSQ_access_exc_lock',
      'PGSQ_relation',
      'PGSQ_extend',
      'PGSQ_page',
      'PGSQ_tuple',
      'PGSQ_transactionid',
      'PGSQ_object',
      'PGSQ_userlock',
      'PGSQ_advisory',
    );
   $output = array();
   foreach ($keys as $key => $short ) {
      # If the value isn't defined, return -1 which is lower than (most graphs')
      # minimum value of 0, so it'll be regarded as a missing value.
      if (isset($status[$key])) {
        $val = $status[$key];
      } elseif (in_array($key, $zero_out_no_responses)) {
        $val = 0;
      } else {
        $val = -1;
      }
      $output[] = "$short:$val";
   }
   $result = implode(' ', $output);
   if ( $fp ) {
      if ( fwrite($fp, $result) === FALSE ) {
         die("Can't write '$cache_file'");
      }
      fclose($fp);
   }
   return $result;
}


# ============================================================================
# Returns a bigint from two ulint or a single hex number.  This is tested in
# t/mysql_stats.php and copied, without tests, to ss_get_by_ssh.php.
# ============================================================================
function make_bigint ($hi, $lo = null) {
   debug(array($hi, $lo));
   if ( is_null($lo) ) {
      # Assume it is a hex string representation.
      return base_convert($hi, 16, 10);
   }
   else {
      $hi = $hi ? $hi : '0'; # Handle empty-string or whatnot
      $lo = $lo ? $lo : '0';
      return big_add(big_multiply($hi, 4294967296), $lo);
   }
}

# ============================================================================
# Extracts the numbers from a string.  You can't reliably do this by casting to
# an int, because numbers that are bigger than PHP's int (varies by platform)
# will be truncated.  And you can't use sprintf(%u) either, because the maximum
# value that will return on some platforms is 4022289582.  So this just handles
# them as a string instead.  It extracts digits until it finds a non-digit and
# quits.  This is tested in t/mysql_stats.php and copied, without tests, to
# ss_get_by_ssh.php.
# ============================================================================
function to_int ( $str ) {
   debug($str);
   global $debug;
   preg_match('{(\d+)}', $str, $m);
   if ( isset($m[1]) ) {
      return $m[1];
   }
   elseif ( $debug ) {
      print_r(debug_backtrace());
   }
   else {
      return 0;
   }
}

# ============================================================================
# Wrap mysql_query in error-handling, and instead of returning the result,
# return an array of arrays in the result.
# ============================================================================
function run_query($sql, $conn) {
   global $debug, $pg_locks, $user, $pass, $db, $host, $ssh_user, $port;
   if ($ssh_user) {
     debug("Using ssh as $ssh_user");
     $pass_string = strlen($pass) > 0 ? "-W $pass" : '';
     $command = "ssh -l $ssh_user $host \"psql -U $user $pass_string $db -p $port -A -c \\\"$sql\\\"\"";
     $command_output = array();
     $result_array = array();
     exec($command, $command_output, $result);
     if ($result == 0) {
       $headers = explode("|", $command_output[0]);
       $headers_passed = false;
       foreach($command_output as $row) {
         if (!$headers_passed) {
           $headers_passed = true;
           continue;
         }
         if (preg_match("/^\(\d+\s+rows?\)$/", $row)) {
           break;
         }
         $header_index = 0;
         $row_hash = array();
         $column_data = explode("|", $row);
         foreach ($column_data as $column) {
           $header = $headers[$header_index++];
           $row_hash[$header] = $column;
         }
         array_push($result_array, $row_hash);
       }
     } else {
       die("Error executing $sql: ".implode("\n", $command_output));
     }
     return $result_array;
     } else {
       debug($sql);
       $result = pg_query($conn, $sql);
       if ( $debug ) {
          $error = pg_last_error($conn);
          if ( $error ) {
             debug(array($sql, $error));
             die("SQLERR $error in $sql");
          }
       }

       $array = array();
       while ( $row = @pg_fetch_array($result, null, PGSQL_ASSOC) ) {
          $array[] = $row;
       }
       debug(array($sql, $array));
       return $array;
     }
}

# ============================================================================
# Safely increments a value that might be null.
# ============================================================================
function increment(&$arr, $key, $howmuch) {
   debug(array($key, $howmuch));
   if ( array_key_exists($key, $arr) && isset($arr[$key]) ) {
      $arr[$key] = big_add($arr[$key], $howmuch);
   }
   else {
      $arr[$key] = $howmuch;
   }
}

# ============================================================================
# Multiply two big integers together as accurately as possible with reasonable
# effort.  This is tested in t/mysql_stats.php and copied, without tests, to
# ss_get_by_ssh.php.  $force is for testability.
# ============================================================================
function big_multiply ($left, $right, $force = null) {
   if ( function_exists("gmp_mul") && (is_null($force) || $force == 'gmp') ) {
      debug(array('gmp_mul', $left, $right));
      return gmp_strval( gmp_mul( $left, $right ));
   }
   elseif ( function_exists("bcmul") && (is_null($force) || $force == 'bc') ) {
      debug(array('bcmul', $left, $right));
      return bcmul( $left, $right );
   }
   else { # Or $force == 'something else'
      debug(array('sprintf', $left, $right));
      return sprintf("%.0f", $left * $right);
   }
}

# ============================================================================
# Subtract two big integers as accurately as possible with reasonable effort.
# This is tested in t/mysql_stats.php and copied, without tests, to
# ss_get_by_ssh.php.  $force is for testability.
# ============================================================================
function big_sub ($left, $right, $force = null) {
   debug(array($left, $right));
   if ( is_null($left)  ) { $left = 0; }
   if ( is_null($right) ) { $right = 0; }
   if ( function_exists("gmp_sub") && (is_null($force) || $force == 'gmp')) {
      debug(array('gmp_sub', $left, $right));
      return gmp_strval( gmp_sub( $left, $right ));
   }
   elseif ( function_exists("bcsub") && (is_null($force) || $force == 'bc')) {
      debug(array('bcsub', $left, $right));
      return bcsub( $left, $right );
   }
   else { # Or $force == 'something else'
      debug(array('to_int', $left, $right));
      return to_int($left - $right);
   }
}

# ============================================================================
# Add two big integers together as accurately as possible with reasonable
# effort.  This is tested in t/mysql_stats.php and copied, without tests, to
# ss_get_by_ssh.php.  $force is for testability.
# ============================================================================
function big_add ($left, $right, $force = null) {
   if ( is_null($left)  ) { $left = 0; }
   if ( is_null($right) ) { $right = 0; }
   if ( function_exists("gmp_add") && (is_null($force) || $force == 'gmp')) {
      debug(array('gmp_add', $left, $right));
      return gmp_strval( gmp_add( $left, $right ));
   }
   elseif ( function_exists("bcadd") && (is_null($force) || $force == 'bc')) {
      debug(array('bcadd', $left, $right));
      return bcadd( $left, $right );
   }
   else { # Or $force == 'something else'
      debug(array('to_int', $left, $right));
      return to_int($left + $right);
   }
}

# ============================================================================
# Changes e.g. AccessShareLock to access_share_lock, for changing strings
# returned from db to make standardized variable names
# ============================================================================
function camelcase_to_underscore ($string) {
  $string = preg_replace('/(?<=\\w)(?=[A-Z])/',"_$1", $string);
  $string = strtolower($string);
  return $string;
}

# ============================================================================
# Writes to a debugging log.
# ============================================================================
function debug($val) {
   global $debug_log;
   if ( !$debug_log ) {
      return;
   }
   if ( $fp = fopen($debug_log, 'a+') ) {
      $trace = debug_backtrace();
      $calls = array();
      $i    = 0;
      $line = 0;
      $file = '';
      foreach ( debug_backtrace() as $arr ) {
         if ( $i++ ) {
            $calls[] = "$arr[function]() at $file:$line";
         }
         $line = array_key_exists('line', $arr) ? $arr['line'] : '?';
         $file = array_key_exists('file', $arr) ? $arr['file'] : '?';
      }
      if ( !count($calls) ) {
         $calls[] = "at $file:$line";
      }
      fwrite($fp, date('Y-m-d H:i:s') . ' ' . implode(' <- ', $calls));
      fwrite($fp, "\n" . var_export($val, TRUE) . "\n");
      fclose($fp);
   }
   else { # Disable logging
      print("Warning: disabling debug logging to $debug_log\n");
      $debug_log = FALSE;
   }
}


