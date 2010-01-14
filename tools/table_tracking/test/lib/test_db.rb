require 'active_record'
ActiveRecord::Migration.verbose = false
require 'ttt/db'
require 'ttt/collector'
require 'ttt/information_schema'
require 'yaml'

# Monkey patch ActiveRecord::Migration
# To use InformationSchema.connection in tests.
# N.B: Only if inheritance doesn't work.
# module ActiveRecord
#   class Migration
#     def self.connection
#       TTT::InformationSchema.connection
#     end
#   end
# end
class TestMigration < ActiveRecord::Migration
  @@verbose = false
  def self.connection
    TTT::InformationSchema.connection
  end
end

# Standard test migration class
# Makes a simple table for testing with.
class CreateTestDataTable < TestMigration
  def self.up
    create_database('test')
    create_table('test.test_data') do |t|
      t.string :name, :limit => 5
      t.string :value, :limit => 20
    end
  end
  def self.down
    drop_database('test')
    #drop_table 'test.test_data'
  end
end

module TestDbHelper
  def test_connect
    @ttt_config = YAML.load_file(ENV['TTT_CONFIG'] ? ENV['TTT_CONFIG'] : "#{Dir.pwd}/dev-config.yml")
    @ttt_config['ttt_connection'] = { :adapter => 'sqlite3', :database => Dir.pwd + '/test-db.sqlite3' }
    ActiveRecord::Base.logger = ActiveSupport::BufferedLogger.new(
      STDOUT,
      ENV['TTT_DEBUG'].to_i == 1 ?
      ActiveSupport::BufferedLogger::Severity::DEBUG :
      ActiveSupport::BufferedLogger::Severity::INFO
    )
    TTT::Db.open(@ttt_config)
    TTT::Db.migrate
  end

  def test_connect_is(host)
    TTT::InformationSchema.connect(host, @ttt_config)
  end

  def test_migration(migClass)
    @migs ||= []
    if @migs[-1] == migClass
      return true
    end
    migClass.migrate(:up)
    @migs.push migClass
  end

  def test_unmigrate(migClass)
    if @migs[-1] != migClass
      return false
    end
    migClass.migrate(:down) unless @migs[-1] != migClass
    @migs.pop
  end

  def truncate_test_tables
    ActiveRecord::Base.connection.select_values("SELECT tbl_name FROM sqlite_master WHERE type='table' AND NOT name = 'sqlite_sequence'").each do |v|
      next if v == 'schema_migrations'
      ActiveRecord::Base.connection.execute(%Q{DELETE FROM "#{v}"}, "truncate drop #{v}")
    end
  end

  def test_cleanup
    return if ENV['TTT_NO_TEST_CLEANUP']
    # Clean up test TTT instance
    ActiveRecord::Base.connection.select_values("SELECT tbl_name FROM sqlite_master WHERE type='table' AND NOT name = 'sqlite_sequence'").each do |v|
      #ActiveRecord::Base.connection.execute(%Q{DROP TABLE "#{v}"}, "cleanup: drop #{v}")
    end

    # Cleanup test DSN instance
    if TTT::InformationSchema.get_connected_host
      unless @migs.nil?
        @migs.reverse.each do |m|
          m.migrate(:down)
        end
      else
        puts "No migrations reverted"
      end
      TTT::InformationSchema.connection.select_values("SHOW DATABASES").each do |d|
        next if d == 'mysql' or d == 'information_schema'
        TTT::InformationSchema.connection.execute(%Q{DROP DATABASE `#{d}`}, "cleanup: drop `#{d}`")
      end
    end
  end
end
