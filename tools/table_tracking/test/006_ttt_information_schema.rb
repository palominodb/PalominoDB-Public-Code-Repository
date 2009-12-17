require 'ttt/db'
require 'ttt/information_schema'
require 'yaml'

describe TTT::InformationSchema do
  before :all do
    @ttt_config = YAML.load_file(ENV['TTT_CONFIG'] ? ENV['TTT_CONFIG'] : "#{Dir.pwd}/dev-config.yml")
    ActiveRecord::Base.logger = ActiveSupport::BufferedLogger.new(
      STDOUT,
      ENV['TTT_DEBUG'].to_i == 1 ?
      ActiveSupport::BufferedLogger::Severity::DEBUG :
      ActiveSupport::BufferedLogger::Severity::INFO)
      TTT::Db.open(@ttt_config)
      TTT::InformationSchema.connect('localhost', @ttt_config)
  end
  it "should throw Exception on find()" do
    caught_excep=false
    begin
      InformationSchema.find(:all)
    rescue Exception
      caught_excep=true
    end
    caught_excep.should == true
  end
end

describe TTT::TABLE do
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
  it "should throw Exception on find()" do
    caught_excep=false
    begin
      TTT::TABLE.find(:all)
    rescue Exception
      caught_excep=true
    end
    caught_excep.should == true
  end

  it "should find `information_schema`.`TABLES` table" do
    TTT::TABLE.get('information_schema', 'TABLES').class.should == TTT::TABLE
  end

  {:name => 'TABLES', :engine => 'MEMORY', :table_type => 'SYSTEM VIEW'}.each do |k,v|
    it "`information_schema`.`TABLES` table #{k} should be #{v}" do
      TTT::TABLE.get('information_schema','TABLES').send(k).should == v
    end
  end

  it "all should find `test`.`test_data`" do
    TTT::TABLE.all.any? { |t| t.schema == "test" and t.name == "test_data" }.should == true
  end
end
