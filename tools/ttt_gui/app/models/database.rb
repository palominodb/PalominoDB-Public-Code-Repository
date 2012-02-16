require 'ttt'
require 'ttt/collector'
require 'ttt/server'
require 'set'
class Database
  attr_reader :stats
  def self.find(server,name)
    db=TTT::Server.find_by_name(server).schemas.find_by_name(name)
    stats={}
    unless db.nil?
      TTT::TrackingTable.tables.each do |s,k|
        tmp=k.find_most_recent_versions({:conditions => ['server = ?', db.server.name]}, :latest)
        tmp=tmp.select { |ts| ts.database_name == name }
        stats[k]=tmp
      end
      self.new(db, stats)
    else
      nil
    end
  end
  def self.all
    TTT::Schema.all(:order => 'cached_size DESC').map { |s| self.find(s.server.name, s.name) }
  end

  def server
    @db.server
  end

  def name
    @db.name
  end

  def hash
    (server+(name.nil? ? "NULL" : name)).hash
  end

  def self.get_size(server,database)
    TTT::Server.find_by_name(server).schemas.find_by_name(database).cached_size
  end

  def get_size
    @db.cached_size
  end

  def tables_full
    TTT::Server.find_by_name(server).schemas.find_by_name(name).tables.map do |t|
      Table.find(server,name, t.name)
    end
  end

  def tables
    @db.tables
  end

  def initialize(db,stats)
    @db=db
    @stats=stats
  end
end
