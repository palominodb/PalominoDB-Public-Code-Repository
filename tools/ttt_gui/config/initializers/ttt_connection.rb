require 'ttt/db'
TTT_CONFIG_PATH=YAML.load_file("#{RAILS_ROOT}/config/ttt_config.yml")["config_path"]
TTT_CONFIG=YAML.load_file(TTT_CONFIG_PATH)
TTT::Db.open(TTT_CONFIG)
