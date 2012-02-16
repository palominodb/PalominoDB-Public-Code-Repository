node "ops.example.com" {
  include palomino
  include palomino::processlogger
  include palomino::querysniper
  include palomino::sql_profiler

  file { "${palomino::bin_dir}/qs.cfg":
    owner => 'root',
    group => 'root',
    mode  => '0644',
    source => 'puppet:///modules/palomino/qs.cfg',
  }

  file { "${palomino::cfg_dir}/sql_profiler":
    ensure => 'directory',
    mode   => '0755',
    owner  => 'root',
    group  => 'root',
  }

  Palomino::Querysniper::Sniper_host <<| tag == 'on_ops' |>>
  Palomino::Processlogger::Log_host <<| tag == 'on_ops' |>>

  define profile($log_path, $ttt_name='') {
    palomino::sql_profiler::profile { $name:
      cfg_file  => "${palomino::cfg_dir}/sql_profiler/${name}.cfg",
      log_path =>  $log_path,
      email_to => 'eng-db@palominodb.com',
      ttt_name => $ttt_name ? {
        '' => $name,
        default => $ttt_name,
      },
    }
  }

  ## Slow Query Profiling

  profile { "db0.example.com":
    log_path => '/var/lib/mysql/slow.log.1',
  }
}
