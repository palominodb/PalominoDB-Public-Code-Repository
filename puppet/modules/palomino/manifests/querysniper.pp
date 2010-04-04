class palomino::querysniper {
  include palomino
  file { "${palomino::bin_dir}/QuerySniper.pm":
    owner => 'root',
    group => 'root',
    mode  => '0644',
    source => 'puppet:///modules/palomino/QuerySniper.pm',
  }
  define sniper_host($watch_interval, $host_cnf, $sniper_rules, $ensure = 'running') {
    service { "querysniper_${name}":
      provider => "base",
      start    => "perl -I ${palomino::bin_dir} ${palomino::mk_loadavg} -F $host_cnf --watch 'Processlist:command:Query:time:>:-1' --plugin 'QuerySniper;$sniper_rules' --host $name --interval $watch_interval --daemonize --pid /tmp/querysniper_${name}.pid",
      stop     => "kill `cat /tmp/querysniper_${name}.pid`",
      status   => "ps auxxx | grep -E '.*QuerySniper.*${name}' | grep -v grep",
      ensure   => $ensure,
      subscribe => File["$sniper_rules", "${palomino::bin_dir}/QuerySniper.pm"],
    }
  }
}
