define querysniper() {
  include palomino::querysniper
  @@palomino::querysniper::sniper_host { $name:
    watch_interval => 60,
    host_cnf       => "${palomino::cfg_dir}/ops.cnf",
    sniper_rules   => "${palomino::cfg_dir}/qs.cfg",
    ensure         => "running",
    tag            => "on_ops",
  }
}
