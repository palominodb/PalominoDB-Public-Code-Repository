define proclog() {
  include palomino::processlogger
  @@palomino::processlogger::log_host { $name:
    watch_interval => 300,
    thread_limit   => 1,
    host_cnf       => "${palomino::cfg_dir}/ops.cnf",
    log_dsn        => "h=localhost,F=${palomino::cfg_dir}/ops.cnf,D=palomino,t=process_list,i=3",
    ensure         => 'running',
    tag            => 'on_ops',
  }
}
