require 'ttt/db'
require 'ttt/table_user'
require 'yaml'

describe TTT::TableUser do
  before(:all) do
    @ttt_config = YAML.load_file(ENV['TTT_CONFIG'] ? ENV['TTT_CONFIG'] : "#{Dir.pwd}/dev-config.yml")
    ActiveRecord::Base.logger = ActiveSupport::BufferedLogger.new(STDOUT, ENV['TTT_DEBUG'].to_i == 1 ? ActiveSupport::BufferedLogger::Severity::DEBUG : ActiveSupport::BufferedLogger::Severity::INFO)
    TTT::Db.open(@ttt_config)
    TTT::TableUser.record_timestamps = false
  end
end
