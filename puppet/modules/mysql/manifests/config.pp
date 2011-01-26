class mysql::config {
  require mysql
  # Default to placing in /tmp for safety.
  # Location can be set explicily inside including classes.
  $dest = $mysql_mycnf_dest  ? {
    '' => "/tmp/my.cnf.puppet_managed",
    "sysdefault" => $mysql::my_cnf_path,
    default => $mysql_mycnf_dest,
  }
  file { 'my.cnf':
    path   => $dest,
    ensure => "file",
    owner => "root",
    group => $operatingsystem ? {
      freebsd => "wheel",
      default => "root",
    },
    source => "puppet:///git-config/mysql/$ipaddress/my.cnf",
  }
}

