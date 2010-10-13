class perl::packages {
  $centos_packages = [ 'perl-Digest-SHA' ]

  $to_install = $operatingsystem ? {
    'centos' => $centos_packages,
    default  => [],
  }
  package { $to_install:
    ensure => 'installed',
  }
}
