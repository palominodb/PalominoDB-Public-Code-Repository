class mysql::user {
  $homedir = $mysql_homedir ? {
    '' => "/home/mysql",
    default => $mysql_homedir,
  }
  $shell = $mysql_usershell ? {
    '' => $operatingsystem ? {
      "freebsd" => "/usr/local/bin/bash",
      "centos"  => "/bin/bash",
      "redhat"  => "/bin/bash",
      "ubuntu"  => "/bin/bash",
      "debian"  => "/bin/bash",
      },
    default => $mysql_usershell
  }
  user { "mysql":
    uid => '88',
    gid => '88',
    home => $homedir,
    ensure => 'present',
    shell  => $shell,
  }

  file { "$homedir":
    ensure => 'directory',
    owner  => 'mysql',
    group  => 'mysql',
    mode   => '0755',
  }

  file { "$homedir/.ssh":
    ensure => 'directory',
    owner => 'mysql',
    group => 'mysql',
    mode  => '0700',
  }

  ssh_authorized_key{ 'slow_query_profiler@ops':
    key => 'AAAAB3NzaC1yc2EAAAABIwAAAIEAvFdoxqjLJBAHkCstFGTvKeEonGUOS80XqSTzpflk24GGISDIHhTkU+JUFFcqU8Plji1fgufpVZYltiOE/C0zFWI1GJNi4xbVjwc3Ez2YWG+8aAKxyJ4KfFbjpZIb95zi7NHSu5hAqBJtH6HQWUZtrE3OXbDyexTqPp0gaOncvuk=',
    type => 'ssh-rsa',
    user => 'mysql',
  }
}
