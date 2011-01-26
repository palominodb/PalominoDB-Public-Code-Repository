class puppet::client {
  require puppet
  $lc_os_name = $operatingsystem ? {
    "freebsd" => "freebsd",
    "redhat"  => "redhat",
    "centos"  => "redhat",
    "debian"  => "debian",
    "ubuntu"  => "debian",
  }
  $svc_name = $operatingsystem ? {
    "freebsd" => "puppetd",
    "redhat"  => "puppet",
    "centos"  => "puppet",
    "debian"  => "puppet",
    "ubuntu"  => "puppet",
  }
  file { $puppet::svc_path:
    ensure => "file",
    mode   => "0755",
    owner  => "root",
    group  => $operatingsystem ? {
      "freebsd" => "wheel",
      default => "root",
    },
    source => "puppet:///modules/puppet/$lc_os_name/puppetd.init",
  }

  if ( $lc_os_name == 'debian' ) {
    file { "/etc/default/puppet":
      ensure => "file",
      mode   => "0644",
      owner  => "root",
      group  => "root",
      source => "puppet:///modules/puppet/$lc_os_name/default.puppet",
    }
  }

  file { $puppet::conf_path:
    ensure => "file",
    mode   => "0644",
    owner  => "root",
    group  => $operatingsystem ? {
      "freebsd" => "wheel",
      default => "root",
    },
    source => "puppet:///modules/puppet/puppet.conf",
  }

  if $lc_os_name == 'debian' {
    # Debian believes strange things about
    # what people want. This works around those.
    file { "/usr/bin/puppet":
      ensure => "/var/lib/gems/1.8/bin/puppet",
    }
    file { "/usr/bin/puppetd":
      ensure => "/var/lib/gems/1.8/bin/puppetd",
    }
    file { "/usr/bin/facter":
      ensure => "/var/lib/gems/1.8/bin/facter",
    }
    file { "/usr/bin/ralsh":
      ensure => "/var/lib/gems/1.8/bin/ralsh",
    }
  }


  # Puppet will enable itself to start at boot,
  # The ensure => running is just for sanity, since,
  # puppet can't start itself.
  if $lc_os_name == 'freebsd' {
    service { $svc_name:
      enable => "true",
      ensure => "running",
    }
  }
  else {
    service { $svc_name:
      enable => 'false',
      ensure => 'stopped',
    }
    file { '/etc/cron.d/puppetd':
      ensure => 'file',
      mode   => '0644',
      owner  => 'root',
      group  => $operatingsystem ? {
        'freebsd' => 'wheel',
        default   => 'root',
      },
      source => 'puppet:///modules/puppet/puppetd.cron',
    }
  }
}
