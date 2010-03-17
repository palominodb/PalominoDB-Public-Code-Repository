class palomino::sql_profiler {
  include palomino
  file { "${palomino::bin_dir}/sql_profiler.sh":
    owner => 'root',
    group => 'root',
    mode  => '0755',
    source => 'puppet:///modules/palomino/sql_profiler.sh',
  }

  define profile($log_path, $ttt_name, $cfg_file, $email_to ) {
    file { "$cfg_file":
      ensure => "file",
      mode   => "0644",
      owner  => "root",
      group  => "root",
      content => template("palomino/sql_profiler_cfg.erb"),
    }
  }
}
