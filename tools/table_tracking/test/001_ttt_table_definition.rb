require 'ttt/db'
require 'ttt/table_definition'
require 'yaml'

describe TTT::TableDefinition do
  def create_entry(serv,db,table,create,created_at=Time.now, updated_at=Time.now)
    TTT::TableDefinition.create do |t|
      t.server = serv
      t.database_name = db
      t.table_name = table
      t.run_time = Time.now
      t.create_syntax = create
      t.created_at = created_at
      t.updated_at = updated_at
    end
  end

  def do_with_rollback
    TTT::TableDefinition.transaction do
      yield
      raise ActiveRecord::Rollback, "Done."
    end
  end
  before(:all) do
    @ttt_config = YAML.load_file(ENV['TTT_CONFIG'] ? ENV['TTT_CONFIG'] : "#{Dir.pwd}/dev-config.yml")
    ActiveRecord::Base.logger = ActiveSupport::BufferedLogger.new(STDOUT, ENV['TTT_DEBUG'].to_i == 1 ? ActiveSupport::BufferedLogger::Severity::DEBUG : ActiveSupport::BufferedLogger::Severity::INFO)
    TTT::Db.open(@ttt_config)
    TTT::TableDefinition.record_timestamps = false
  end


  it "method 'new?' should report new" do
    do_with_rollback do
      newentry=create_entry("fakedb", "fake", "rspec", "CREATE TABLE rspec (id INTEGER PRIMARY KEY AUTO_INCREMENT)")
      newentry.save
      newentry.new?.should == true
    end
  end

  it "method 'status' should report ':new' for new tables" do
    do_with_rollback do
      newentry=create_entry("fakedb", "fake", "rspec", "CREATE TABLE rspec (id INTEGER PRIMARY KEY AUTO_INCREMENT)")
      newentry.save
      newentry.status.should == :new
    end
  end

  it "method 'tchanged?' should report 'true' for changed tables" do
    do_with_rollback do
      newentry1=create_entry("fakedb", "fake", "rspec", "CREATE TABLE rspec (id INTEGER PRIMARY KEY AUTO_INCREMENT)")
      newentry2=create_entry("fakedb", "fake", "rspec", "CREATE TABLE rspec (id INTEGER PRIMARY KEY AUTO_INCREMENT, value VARCHAR(128))")
      newentry1.save
      newentry2.save
      newentry2.tchanged?.should == true
      newentry2.status.should == :changed
    end
  end

  it "method 'deleted?' should report 'true' for deleted tables" do
    do_with_rollback do
      newentry1=create_entry("fakedb", "fake", "rspec", "CREATE TABLE rspec (id INTEGER PRIMARY KEY AUTO_INCREMENT)")
      newentry2=create_entry("fakedb", "fake", "rspec", nil, Time.now, nil)
      newentry1.save
      newentry2.save
      newentry2.deleted?.should == true
    end
  end

end
