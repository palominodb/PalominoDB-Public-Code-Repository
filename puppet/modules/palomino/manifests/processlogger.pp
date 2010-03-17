class palomino::processlogger {
  include palomino
  # perl /usr/local/pdb/bin/mk-loadavg -F /usr/local/pdb/bin/ops.cnf --watch Status:status:Threads_connected:>:1000 --plugin ProcesslistLogger;h=localhost,u=log,D=palomino,t=process_list,p=noop -h 10.10.1.142 --interval 30 --daemonize
  define log_host($watch_interval, $host_cnf, $log_dsn, $thread_limit, $ensure = 'running') {
    service { "processlogger_${name}":
      provider => "base",
      start    => "perl -I ${palomino::bin_dir} ${palomino::mk_loadavg} -F $host_cnf --watch 'Status:status:Threads_connected:>:$thread_limit' --plugin 'ProcesslistLogger;$log_dsn' --host $name --interval $watch_interval --daemonize --pid /tmp/processlist_logger_${name}.pid",
      stop     => "kill `cat /tmp/processlist_logger_${name}.pid`",
      status   => "ps auxxx | grep -E '.*ProcesslistLogger.*${name}' | grep -v grep",
      ensure   => $ensure,
    }
  }
}
