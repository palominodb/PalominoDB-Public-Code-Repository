class maatkit::heartbeat {
  include 'platform'
  schedule { 'kill-heartbeat':
    period => 'hourly',
  }
  $mk_heartbeat_ensure = $mk_heartbeat_ensure ? {
    ''      => 'running',
    default => $mk_heartbeat_ensure,
  }
  $mk_heartbeat_cnf = $mk_heartbeat_cnf ? {
    ''      => "/home/mysql/heartbeat.cnf",
    default => $mk_heartbeat_cnf,
  }
  $mk_heartbeat_db = $mk_heartbeat_db ? {
    ''      => 'heartbeat',
    default => $mk_heartbeat_db,
  }
  $mk_heartbeat_table = $mk_heartbeat_table ? {
    ''      => 'heartbeat',
    default => $mk_heartbeat_table,
  }
  $mk_heartbeat_pidfile = $mk_heartbeat_pidfile ? {
    ''      => '/tmp/mk-heartbeat.pid',
    default => $mk_heartbeat_pidfile,
  }
  
  file { $mk_heartbeat_cnf:
    ensure => 'file',
    owner  => 'root',
    group  => $platform::root_user_group,
    mode   => '0600',
  }
  service { "mk-heartbeat":
    provider => "base",
    start    => "mk-heartbeat --daemonize --pid $mk_heartbeat_pidfile --update -F $mk_heartbeat_cnf --database $mk_heartbeat_db --table $mk_heartbeat_table",
    stop     => "kill `cat /tmp/mk-heartbeat.pid`",
    ensure   => $mk_heartbeat_ensure,
    require  => [File[$mk_heartbeat_cnf],Exec[kill_heartbeat]],
  }

  exec { "/bin/kill `cat /tmp/mk-heartbeat.pid` ; echo 0":
    schedule => 'kill-heartbeat',
    alias    => 'kill_heartbeat',
    before   => Service['mk-heartbeat'],
  }
}
