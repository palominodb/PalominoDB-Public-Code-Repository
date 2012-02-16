class sysctl {
  $dest = $sysctl_dest ? {
    '' => "/tmp/sysctl.conf.puppet_managed",
    "sysdefault" => "/etc/sysctl.conf",
    default => $sysctl_dest,
  }

  file { "$dest":
    ensure => "file",
    owner  => "root",
    group  => "$operatingsystem ? {
      freebsd => "wheel",
      default => "root",
    },
    source => "puppet:///git-config/os/$ipaddress/sysctl.conf",
  }
}
