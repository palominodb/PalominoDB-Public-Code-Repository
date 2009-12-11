class puppet {
  # Settings global to puppet go here.

  # This is for sanity. Obviously puppet
  # won't function or be able to install
  # itself if it is missing!
  package { ["puppet", "facter"]:
    ensure => "installed",
    provider => "gem"
  }

  # Other modules may need to know these things.
  $svc_path = $operatingsystem ? {
    "freebsd" => "/etc/rc.d/puppet",
    "centos"  => "/etc/init.d/puppet",
    "redhat"  => "/etc/init.d/puppet",
    "debian"  => "/etc/init.d/puppet",
  }

  $conf_path = $operatingsystem ? {
    "freebsd" => "/etc/puppet/puppet.conf",
    "centos"  => "/etc/puppet/puppet.conf",
    "redhat"  => "/etc/puppet/puppet.conf",
    "debian"  => "/etc/puppet/puppet.conf",
  }
}
