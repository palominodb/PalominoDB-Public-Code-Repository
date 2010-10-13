class mysqlctl::package {
  $pkg_version = '0.04-2'
  $pkg_vendor  = 'site'
  package {
    'mysqlctl':
      ensure => $pkg_version;
    "mysqlctl-init-$pkg_vendor":
      ensure => $pkg_version,
      require => Package[mysqlctl];
  }
    
  service { 'mysql':
    enable  => 'false',
    require => Package["mysqlctl-init-$pkg_vendor"],
  }

  service { "$pkg_vendor-mysql":
    enable     => 'true',
    hasstatus  => 'true',
    hasrestart => 'false',
    require    => [Service[mysql], Package["mysqlctl-init-$pkg_vendor"]],
  }

  # Optional:
  #file { '/etc/init.d/mysql':
  #  ensure => 'file',
  #  owner  => 'root',
  #  group  => 'root',
  #  mode   => '0755',
  #  content => "#!/bin/sh\necho Use /etc/init.d/$pkg_vendor-mysql instead.\nexit 1",
  #}
}
