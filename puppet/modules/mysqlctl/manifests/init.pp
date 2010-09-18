class mysqlctl {
  package {
    'mysqlctl':
      ensure => 'latest';
    'mysqlctl-init-site':
      ensure => 'latest';
  }

  file { '/etc/myctl.cnf':
    ensure  => 'file',
    mode    => '0600',
    owner   => 'root',
    group   => 'root',
    content => template("mysqlctl/myctl.cnf.erb"),
    require => Package['mysqlctl'],
  }
}
