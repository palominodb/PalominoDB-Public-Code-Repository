# 004_ttt_table_user.rb
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
require 'ttt/collector'
require 'ttt/table_user'
require 'ttt/collector/user'
require 'test/lib/mysql_migrate_grants'
require 'test/lib/test_db'
require 'yaml'

class TestGlobalUser < TestMigration
  def self.up
    grant_global('guser','localhost', ['select'], nil)
  end

  def self.down
    drop_user('guser', 'localhost')
  end
end

class TestHostUser < TestMigration
  def self.up
    execute("INSERT INTO `mysql`.`host` (Host,Db,Select_priv) VALUES ('localhost', 'test', 'Y')")
  end
  
  def self.down
    execute("DELETE FROM `mysql`.`host` WHERE Host='localhost' AND Db='test'")
  end
end

class TestDbUser < TestMigration
  def self.up
    grant_db('dbuser', 'localhost', 'test', ['select'],nil)
  end
  def self.down
    drop_user('dbuser', 'localhost')
  end
end

class TestTblUser < TestMigration
  def self.up
    grant_tbl('tbluser', 'localhost', 'test', 'test_data', ['select'],nil)
  end
  def self.down
    drop_user('tbluser', 'localhost')
  end
end

class TestTblUser1 < TestMigration
  def self.up
    grant_tbl('tbluser1', 'localhost', 'test', 'test_data', ['select'], nil)
  end
  def self.down
    drop_user('tbluser1', 'localhost')
  end
end

class TestColUser < TestMigration
  def self.up
    grant_col('coluser', 'localhost', 'test', 'test_data', [['select', 'name', 'value']], nil)
  end

  def self.down
    revoke_col('coluser', 'localhost', 'test', 'test_data', [['select', 'name', 'value']])
  end
end

class TestRoutineUser < TestMigration
  def self.up
    grant_proc('routuser', 'localhost', 'test', 'test_proc', ['execute'], nil)
  end

  def self.down
    revoke_proc('routuser', 'localhost', 'test', 'test_proc', ['execute'])
  end
end

class AddGlobalUserPriv1 < TestMigration
  def self.up
    grant_global('guser', 'localhost', ['insert'],nil)
  end

  def self.down
    revoke_global('guser', 'localhost', ['insert'])
  end
end

class AddDbUserPriv1 < TestMigration
  def self.up
    grant_db('dbuser', 'localhost', 'test', ['insert'],nil)
  end

  def self.down
    revoke_db('dbuser', 'localhost', 'test', ['insert'])
  end
end

class AddTblUserPriv1 < TestMigration
  def self.up
    grant_tbl('tbluser', 'localhost', 'test', 'test_data', ['insert'],nil)
  end

  def self.down
    revoke_tbl('tbluser', 'localhost', 'test', 'test_data', ['insert'])
  end
end

class AddTblUserPriv2 < TestMigration
  def self.up
    grant_tbl('tbluser1', 'localhost', 'test', 'test_data', ['insert'],nil)
  end

  def self.down
    revoke_tbl('tbluser1', 'localhost', 'test', 'test_data', ['insert'])
  end
end

class AddColUserPriv1 < TestMigration
  def self.up
    grant_col('coluser', 'localhost', 'test', 'test_data', [['update', 'value']],nil)
  end

  def self.down
    revoke_col('coluser', 'localhost', 'test', 'test_data', [['update', 'value']])
  end
end

class AddRoutineUserPriv1 < TestMigration
  def self.up
    grant_proc('routuser', 'localhost', 'test', 'test_proc', ['alter routine'], nil)
  end
  def self.down
    revoke_proc('routuser', 'localhost', 'test', 'test_proc', ['alter routine'])
  end
end

class AddHostUserPriv1 < TestMigration
  def self.up
    execute("UPDATE `mysql`.`host` SET Insert_priv='Y' WHERE Host='localhost' AND Db='test'")
  end
  
  def self.down
    execute("UPDATE `mysql`.`host` SET Insert_priv='N' WHERE Host='localhost' AND Db='test'")
  end
end


describe TTT::TableUser, 'instances' do
  include TestDbHelper
  before(:all) do
    test_connect
    test_connect_is('localhost')
    TTT::CollectorRegistry.unload
    Kernel.load File.dirname(__FILE__)+"/../lib/ttt/collector/user.rb"
  end

  before do
    truncate_test_tables
  end

  it 'should have a collector' do
    TTT::CollectorRegistry.all[0].stat.should == TTT::TableUser
  end

  # Permission type testing
  { :deleted?     => TTT::TableUser::DELETED_PERMISSION,
    :global_perm? => TTT::TableUser::GLOBAL_PERMISSION,
    :host_perm?   => TTT::TableUser::HOST_PERMISSION,
    :db_perm?     => TTT::TableUser::DB_PERMISSION,
    :table_perm?  => TTT::TableUser::TABLE_PERMISSION,
    :column_perm? => TTT::TableUser::COLUMN_PERMISSION,
    :proc_perm?   => TTT::TableUser::PROC_PERMISSION
  }.each do |p,v|
    it "permission #{p} should work" do
      u=TTT::TableUser.new(:server => 'localhost', :Host => 'localhost', :User => 'test_user')
      u.permtype = TTT::TableUser::RESERVED_PERMISSION2
      u.permtype |= v
      [:Select_priv, :Insert_priv, :Execute_priv, :Update_priv].each { |pr| u[pr] = (rand(2) == 0 ? 'N' : 'Y') }
      u.send(p).should == true
      u.save

      TTT::TableUser.find(u.id)
      u.send(p).should == true
    end
  end

  after(:all) do
    test_cleanup
  end
end

describe TTT::TableUser, 'collection' do
  PRIV_COLUMNS = [
    :Create_priv,
    :Drop_priv,
    :Grant_priv,
    :References_priv,
    :Event_priv,
    :Alter_priv,
    :Delete_priv,
    :Index_priv,
    :Insert_priv,
    :Select_priv,
    :Update_priv,
    :Create_tmp_table_priv,
    :Lock_tables_priv,
    :Trigger_priv,
    :Create_view_priv,
    :Show_view_priv,
    :Alter_routine_priv,
    :Create_routine_priv,
    :Execute_priv,
    :File_priv,
    :Create_user_priv,
    :Process_priv,
    :Reload_priv,
    :Repl_client_priv,
    :Repl_slave_priv,
    :Show_db_priv,
    :Shutdown_priv,
    :Super_priv
  ]
  include TestDbHelper
  before(:all) do
    test_connect
    test_connect_is('localhost')
    TTT::CollectorRegistry.unload
    Kernel.load File.dirname(__FILE__)+"/../lib/ttt/collector/user.rb"
  end

  before do
    truncate_test_tables
  end

  def run_collection
    rd=TTT::CollectionDirector::RunData.new('localhost', [], TTT::CollectorRegistry.all[0], Time.now)
    TTT::CollectorRegistry.all[0].run(rd)
    rd
  end

  def test_privs(user, true_privs)
    user.should_not.nil?
    true_privs.each do |p|
      user[p].should == 'Y'
    end
    PRIV_COLUMNS.each { |c|
      next if true_privs.include? c
      if user[c].nil?
        user[c].should == nil
      else
        user[c].should == 'N'
      end
    }
  end

  it "collection should not report new twice" do
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    rd=run_collection
    rd.changed?.should_not == true
  end

  it "should find 'guser'@'localhost'" do
    test_migration(TestGlobalUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ?', 'guser', 'localhost'])
    test_privs(u, [:Select_priv])
  end
  it "should not error on previously deleted 'guser'@'localhost'" do
    test_migration(TestGlobalUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ?', 'guser', 'localhost'])
    test_privs(u, [:Select_priv])

    test_unmigrate(TestGlobalUser)

    rd=run_collection
    rd.changed?.should == true
    rd.save(1)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ?', 'guser', 'localhost'])
    u.deleted?.should == true

    rd=run_collection
    rd.changed?.should == false
  end

  it "should find 'guser'@'localhost' insert grant" do
    test_migration(TestGlobalUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ?', 'guser', 'localhost'])
    test_privs(u, [:Select_priv])

    test_migration(AddGlobalUserPriv1)

    rd=run_collection
    rd.changed?.should == true
    rd.save(1)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ?', 'guser', 'localhost'])
    test_privs(u, [:Select_priv, :Insert_priv])
  end

  it "should find 'guser'@'localhost' insert revoke" do
    test_migration(TestGlobalUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ?', 'guser', 'localhost'])
    test_privs(u, [:Select_priv])

    test_migration(AddGlobalUserPriv1)

    rd=run_collection
    rd.changed?.should == true
    rd.save(1)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ?', 'guser', 'localhost'])
    test_privs(u, [:Select_priv, :Insert_priv])

    test_unmigrate(AddGlobalUserPriv1)

    rd=run_collection
    rd.changed?.should == true
    rd.save(2)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ?', 'guser', 'localhost'])
    test_privs(u, [:Select_priv])
  end

  it "'guser'@'localhost' is deleted" do
    test_migration(TestGlobalUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ?', 'guser', 'localhost'])
    test_privs(u, [:Select_priv])
    test_unmigrate(TestGlobalUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(1)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ?', 'guser', 'localhost'])
    u.deleted?.should == true
  end

  it "should find 'dbuser'@'localhost'" do
    test_migration(TestDbUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ? AND Db = ?', 'dbuser', 'localhost', 'test'])
    test_privs(u, [:Select_priv])
  end

  it "should find 'dbuser'@'localhost' insert grant" do
    test_migration(TestDbUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    test_migration(AddDbUserPriv1)
    rd=run_collection
    rd.changed?.should == true
    rd.save(1)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ? AND Db = ?', 'dbuser', 'localhost', 'test'])
    test_privs(u, [:Select_priv, :Insert_priv])
  end

  it "should find 'tbluser'@'localhost'" do
    test_migration(CreateTestDataTable)
    test_migration(TestTblUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ? AND Db = ? AND Table_name = ?', 'tbluser', 'localhost', 'test', 'test_data'])
    test_privs(u, [:Select_priv])
  end

  it "should find 'tbluser'@'localhost' insert grant" do
    test_migration(CreateTestDataTable)
    test_migration(TestTblUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ? AND Db = ? AND Table_name = ?', 'tbluser', 'localhost', 'test', 'test_data'])
    test_privs(u, [:Select_priv])
    test_migration(AddTblUserPriv1)
    rd=run_collection
    rd.changed?.should == true
    rd.save(1)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ? AND Db = ? AND Table_name = ?', 'tbluser', 'localhost', 'test', 'test_data'])
    test_privs(u, [:Select_priv, :Insert_priv])
  end

  it "should find `select (name,value)` for 'coluser'@'localhost'" do
    test_migration(CreateTestDataTable)
    test_migration(TestColUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ? AND Db = ? AND Table_name = ? AND Column_name = ?', 'coluser', 'localhost', 'test', 'test_data', 'name'])
    u.should_not == nil
    test_privs(u, [:Select_priv])
  end

  it "should find `update (value)` for 'coluser'@'localhost'" do
    test_migration(CreateTestDataTable)
    test_migration(TestColUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    test_migration(AddColUserPriv1)
    rd=run_collection
    rd.changed?.should == true
    rd.save(1)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ? AND Db = ? AND Table_name = ? AND Column_name = ?', 'coluser', 'localhost', 'test', 'test_data', 'value'])
    u.should_not == nil
    test_privs(u, [:Select_priv, :Update_priv])
  end

  it "should find 'routuser'@'localhost'" do
    test_migration(CreateTestDataTable)
    test_migration(TestRoutineUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ? AND Db = ? AND Routine_name = ?', 'routuser', 'localhost', 'test', 'test_proc'])
    test_privs(u, [:Execute_priv])
  end

  it "should find 'routuser'@'localhost' alter routine grant" do
    test_migration(CreateTestDataTable)
    test_migration(TestRoutineUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    test_migration(AddRoutineUserPriv1)
    rd=run_collection
    rd.changed?.should == true
    rd.save(1)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ? AND Db = ? AND Routine_name = ?', 'routuser', 'localhost', 'test', 'test_proc'])
    test_privs(u, [:Execute_priv, :Alter_routine_priv])
  end

  it "should find host 'localhost'.'test'" do
    test_migration(CreateTestDataTable)
    test_migration(TestHostUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    u=TTT::TableUser.find(:last, :conditions => ['Host = ? AND Db = ?', 'localhost', 'test'])
    test_privs(u, [:Select_priv])
  end

  it "should find host 'localhost'.'test' insert priv" do
    test_migration(CreateTestDataTable)
    test_migration(TestHostUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    test_migration(AddHostUserPriv1)
    rd=run_collection
    rd.changed?.should == true
    rd.save(1)
    u=TTT::TableUser.find(:last, :conditions => ['Host = ? AND Db = ?', 'localhost', 'test'])
    test_privs(u, [:Select_priv, :Insert_priv])
  end

  it 'should raise RuntimeError on unknown column type values' do
    test_migration(TestGlobalUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ?', 'guser', 'localhost'])
    u.permtype = 103 # Some invalid type
    u.save
    test_migration(AddGlobalUserPriv1)
    lambda { run_collection }.should raise_exception(RuntimeError)
  end

  it 'should correctly detect previous versions' do
    test_migration(CreateTestDataTable)
    test_migration(TestTblUser)
    rd=run_collection
    rd.changed?.should == true
    rd.save(0)
    u=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ? AND Db = ? AND Table_name = ?',
                          'tbluser', 'localhost', 'test', 'test_data'])
    test_privs(u, [:Select_priv])
    test_migration(AddTblUserPriv1)
    test_migration(TestTblUser1)
    rd=run_collection
    rd.changed?.should == true
    rd.save(1)
    u2=TTT::TableUser.find(:last, :conditions => ['User = ? AND Host = ? AND Db = ? AND Table_name = ?',
                          'tbluser', 'localhost', 'test', 'test_data'])
    u2.previous_version.id.should == u.id
  end

  after do
    test_cleanup
  end
end
