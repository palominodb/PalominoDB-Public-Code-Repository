class palomino {
  # Where the tools are staged
  $install_dir = '/usr/local/pdb'
  $bin_dir = "$install_dir/bin"
  $cfg_dir = "$install_dir/etc"

  # PalominoDB patched mk-loadavg
  $mk_loadavg = "$bin_dir/mk-loadavg"

  file { $install_dir:
    ensure => 'directory',
    owner => 'root',
    group => 'root',
    mode  => '0755',
  }

  file { $bin_dir:
    ensure => 'directory',
    owner => 'root',
    group => 'root',
    mode  => '0755',
  }

  file { $cfg_dir:
    ensure => 'directory',
    owner => 'root',
    group => 'root',
    mode  => '0755',
  }

  file { $mk_loadavg:
    ensure => 'file',
    owner => 'root',
    group => 'root',
    mode  => '0755',
    source => 'puppet:///modules/palomino/mk-loadavg',
  }

}
