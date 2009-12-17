require 'ttt/db'
require 'ttt/collector'
require 'yaml'

describe TTT::CollectorRun do
  before :all do
    @ttt_config = YAML.load_file(ENV['TTT_CONFIG'] ? ENV['TTT_CONFIG'] : "#{Dir.pwd}/dev-config.yml")
    @ttt_config['dsn_connection'].delete 'password'
    ActiveRecord::Base.logger = ActiveSupport::BufferedLogger.new(
                                STDOUT,
                                ENV['TTT_DEBUG'].to_i == 1 ?
                                ActiveSupport::BufferedLogger::Severity::DEBUG :
                                ActiveSupport::BufferedLogger::Severity::INFO)
    TTT::Db.open(@ttt_config)
    TTT::InformationSchema.connect('localhost', @ttt_config)
  end

  [:definition, :view, :volume].each do |c|
    it "should find #{c}" do
      TTT::CollectorRun.find_by_collector(c).class.should == TTT::CollectorRun
    end

    it "TTT::CollectorRun(#{c}).collector should equal #{c}" do
      TTT::CollectorRun.find_by_collector(c).collector.should == c.to_s
    end
  end
end

describe TTT::Collector do
  before :all do
    @ttt_config = YAML.load_file(ENV['TTT_CONFIG'] ? ENV['TTT_CONFIG'] : "#{Dir.pwd}/dev-config.yml")
    @ttt_config['dsn_connection'].delete 'password'
    ActiveRecord::Base.logger = ActiveSupport::BufferedLogger.new(
                                STDOUT,
                                ENV['TTT_DEBUG'].to_i == 1 ?
                                ActiveSupport::BufferedLogger::Severity::DEBUG :
                                ActiveSupport::BufferedLogger::Severity::INFO)
    TTT::Db.open(@ttt_config)
    TTT::InformationSchema.connect('localhost', @ttt_config)
  end

end

describe TTT::CollectionDirector::TableCache do
  before :all do
    @ttt_config = YAML.load_file(ENV['TTT_CONFIG'] ? ENV['TTT_CONFIG'] : "#{Dir.pwd}/dev-config.yml")
    @ttt_config['dsn_connection'].delete 'password'
    ActiveRecord::Base.logger = ActiveSupport::BufferedLogger.new(
                                STDOUT,
                                ENV['TTT_DEBUG'].to_i == 1 ?
                                ActiveSupport::BufferedLogger::Severity::DEBUG :
                                ActiveSupport::BufferedLogger::Severity::INFO)
    TTT::Db.open(@ttt_config)
    TTT::InformationSchema.connect('localhost', @ttt_config)
  end

  before do
    @cache = TTT::CollectionDirector::TableCache.new(TTT::TABLE.all)
  end
  it "find_by_schema_and_table should find `test`.`test_data`" do
    t=@cache.find_by_schema_and_table("test", "test_data")
    t.class.should == TTT::TABLE
    t.should_not == nil
    t.schema.should == "test"
    t.name.should == "test_data"
  end
end
