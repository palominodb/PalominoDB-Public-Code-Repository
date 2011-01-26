class mysql::mysqlctl {
  require mysql
  file { "/etc/myctl.cnf":
    ensure => "file",
    owner  => "root",
    group  => $operatingsystem ? {
      freebsd => "wheel",
      default => "root",
    },
    content => template("mysql/myctl.cnf.erb"),
  }
}
