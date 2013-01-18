# 001_ttt_table_definition.rb
# Copyright (C) 2013 PalominoDB, Inc.
# 
# You may contact the maintainers at eng@palominodb.com.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

require 'ttt/db'
require 'ttt/table_definition'
require 'yaml'

class MutateTestDataTable < TestMigration
  def self.up
    add_column('test.test_data', :mu, :string, :limit => 10)
  end
  def self.down
  end
end

class CreateTableWithPk < TestMigration
  def self.up
    connection.execute('CREATE DATABASE IF NOT EXISTS test')
    create_table('test.test_pk', :options => 'ENGINE=InnoDB CHARSET=utf8') do |t|
    end
  end
  def self.down
  end
end

class InsertPkRows < TestMigration
  def self.up
    execute("INSERT INTO `test`.`test_pk` () VALUES ()")
    execute("INSERT INTO `test`.`test_pk` () VALUES ()")
    execute("ALTER TABLE `test`.`test_pk` ENGINE=InnoDB")
    execute("INSERT INTO `test`.`test_pk` () VALUES ()")
    execute("INSERT INTO `test`.`test_pk` () VALUES ()")
  end
  def self.down
    execute("TRUNCATE TABLE `test`.`test_pk`");
  end
end

describe TTT::TableDefinition do
  include TestDbHelper
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
  before :all do
    test_connect
    test_connect_is('localhost')
    TTT::TableDefinition.record_timestamps = false
  end

  after :all do
    test_cleanup
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

describe TTT::TableDefinition, 'collect' do
  include TestDbHelper
  before(:all) do
    TTT::CollectorRegistry.load
    @defn_collector = nil
    ObjectSpace.each_object() { |o| @defn_collector=o if o.instance_of? TTT::Collector and o.stat == TTT::TableDefinition }
    test_connect
    test_connect_is('localhost')

    @defn_collector.verbose = true
  end

  before do
    truncate_test_tables
  end

  after do
    test_cleanup
  end

  def run_collection(id, truth, tables=[], at_time=Time.at(0))
    outrd=nil
      TTT::Snapshot.transaction do |txn_id|
        rd=nil
        cd=TTT::CollectionDirector.new(@ttt_config, at_time)
        cd.stub!(:recache_tables!).and_return {
          cd.instance_variable_set("@cached_tables",
                                   TTT::CollectionDirector::TableCache.new(
                                     tables
                                   )
                                  )
        }

        rd=cd.collect('localhost', @defn_collector)
        rd.changed?.should == truth
        rd.save(id)
        outrd=rd
      end
    outrd
  end

  it 'should find test.test_data' do
    test_migration(CreateTestDataTable)
    rd=run_collection(1, true, TTT::TABLE.get('test', 'test_data'), TIMES[0])
    TTT::TableDefinition.find(:last).status.should == :new
  end

  it 'should find test.test_data changed' do
    test_migration(CreateTestDataTable)
    rd=run_collection(1, true, TTT::TABLE.get('test', 'test_data'), TIMES[0])
    test_migration(MutateTestDataTable)
    rd=run_collection(2, true, TTT::TABLE.get('test', 'test_data'), TIMES[1])
    TTT::TableDefinition.find(:last).status.should == :changed
  end

  # this test is to validate that we don't find a table changed due to
  # an auto_increment change per ticket [9babce26e5e802dbc14737404cb73d84d605ef71].
  it 'should not find test.test_pk changed' do
    test_migration(CreateTableWithPk)
    rd=run_collection(1, true, TTT::TABLE.get('test', 'test_pk'), TIMES[0])
    TTT::TableDefinition.find(:last).status.should == :new
    test_migration(InsertPkRows)
    rd=run_collection(2, false, TTT::TABLE.get('test', 'test_pk'), TIMES[1])
    TTT::TableDefinition.find(:last).status.should == :new
  end
end


