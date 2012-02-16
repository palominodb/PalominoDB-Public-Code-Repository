class zrm::socket_server {
  file { "/usr/share/mysql-zrm/plugins/socket-server.conf":
    ensure => "file",
    mode   => "0644",
    owner  => "mysql",
    group  => "mysql",
    source => "puppet:///modules/zrm/socket-server.conf",
  }
}
