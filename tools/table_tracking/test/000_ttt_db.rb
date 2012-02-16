require 'ttt/db'
require 'yaml'

describe TTT::Db, "in the beginning" do
  it 'open must succeed with valid input' do
    ttt_config = YAML.load_file(ENV['TTT_CONFIG'] ? ENV['TTT_CONFIG'] : "#{Dir.pwd}/dev-config.yml")
    ttt_config['ttt_connection'] = { :adapter => 'sqlite3', :database => Dir.pwd + '/test-db.sqlite3' }
    lambda { TTT::Db.open(ttt_config) }.should_not raise_exception
  end

  it 'open must raise an exception with invalid input' do
    lambda { TTT::Db.open({}) }.should raise_exception(ArgumentError)
  end
end

# Validate TTT::Db before running other tests
# Since our test kit relies on it.
# If these don't all pass, then, the results from
# other tests are highly suspect.
describe TTT::Db do
  include TestDbHelper
  before :all do
    test_connect
  end
  after :all do
    test_cleanup
  end

  it 'migrate should create tables' do
    # Just do a sampling. I trust
    lambda {
      TTT::CollectorRun.first
      TTT::TableDefinition.first
      TTT::TableVolume.first
      TTT::TableView.first
      TTT::TableUser.first
      TTT::Snapshot.first
    }.should_not raise_exception
  end
  it 'open_schema should get test.test_data' do
    mysch=nil
    lambda {
      mysch=TTT::Db.open_schema('localhost', 'test', 'test_data')
    }.should_not raise_exception
    mysch.should_not.nil?
  end

  it 'open_schema shoud not get test.not_here' do
    mysch=nil
    mysch=TTT::Db.open_schema('localhost', 'test', 'not_here')
    mysch.should.nil?
  end
end
