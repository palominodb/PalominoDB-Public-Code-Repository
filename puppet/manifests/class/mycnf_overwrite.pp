class mysql::config_overwrite {
  $mysql_mycnf_dest = 'sysdefault'
  require mysql::config
}
