class mysql {
  $my_cnf_path = $operatingsystem ? {
    "freebsd" => "/etc/my.cnf",
    "centos"  => "/etc/my.cnf",
    "redhat"  => "/etc/my.cnf",
    "debian"  => "/etc/mysql/my.cnf",
    "ubuntu"  => "/etc/mysql/my.cnf",
  }
}
