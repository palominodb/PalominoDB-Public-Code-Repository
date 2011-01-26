node "db0.example.com" {
  include puppet::client

  # by default, mysql::config doesn't overwrite
  # the live my.cnf - this is for migration purposes.
  # setting the below variable will make that happen.
  #
  # $mysql_mycnf_dest = 'sysdefault'
  #
  # OR
  # include mysql::config_overwrite
  #
  # will do the same thing.
  include mysql::config

  # to manage sysctl.conf:
  #
  # include sysctl
  #
  # OR
  #
  # include sysctl_overwrite

  ## Example config

  # if mk-heartbeat is in use, puppet can do that.
  #
  # $mk_heartbeat_ensure = 'running'
  # include maatkit::heartbeat


  # processlist logging can help with post-mortem analysis
  # ensure processlist logging is happening
  # requires storedconfigs
  #
  # proclog { $hostname: }

  # for corp/business intelligence hosts,
  # query sniping can help keep load down, and prevent
  # bad queries from running amok.
  # requires storedconfigs
  #
  # include palomino::querysniper
  # querysniper { $hostname: }

  # our advanced init script:
  # include mysql::mysqlctl
}
