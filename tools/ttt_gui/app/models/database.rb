require 'ttt'
require 'ttt/collector'
require 'ttt/server'
require 'set'
class Database
  attr_reader :server
  attr_reader :name
  attr_reader :stats
  def self.find(server,name)
    db=TTT::Server.find_by_name(server).schemas.find_by_name(name)
    stats={}
    unless db.nil?
      TTT::TrackingTable.tables.each do |s,k|
        stats[s]=k.find_most_recent_versions({:conditions => ['server = ? and database_name = ?', db.server.name, db.name]})
        #stats[s]=k.find(:all, :conditions => ["server = ? and database_name = ? and run_time = ?", server, name, TTT::Collector.get_last_run(s)])
      end
      self.new(server,name, stats)
    else
      nil
    end
  end
  def self.all
    TTT::Schema.all(:order => 'cached_size DESC').map { |s| self.find(s.server.name, s.name) }
    #dbs=Set.new
    ##TTT::TrackingTable.tables.each do |s,k|
    ##  stats[s]=k.find(:all, :conditions => ["server = ? and database_name = ? and run_time = ?", server, name, TTT::Collector.get_last_run(s)])
    ##end

    #TTT::TrackingTable.tables.each do |s,tt|
    #  dbs.merge tt.find(:all, :select => "server, database_name", :group => 'server, database_name', :conditions => ["run_time = ?", TTT::Collector.get_last_run(s)]).map { |s|
    #    self.find(s.server,s.database_name)
    #  }
    #end

    #dbs
  end

  def hash
    (server+(name.nil? ? "NULL" : name)).hash
  end

  def self.get_size(server,database)
    TTT::Server.find_by_name(server).schemas.find_by_name(database).cached_size
    #TTT::TableVolume.aggregate_by_schema(server,database)
  end

  def get_size
    Database.get_size(server,name)
    #size=0
    #@stats[:volume].each do |d|
    #  size+=d.size
    #end
    #size
  end

  def tables_full
    TTT::Server.find_by_name(server).schemas.find_by_name(name).tables.map do |t|
      Table.find(server,name, t.name)
    end
  end

  def tables
    TTT::Server.find_by_name(server).schemas.find_by_name(name).tables
  end

  #def tables
  #  tbls=[]
  #  @stats.collect do |k,v|
  #    tbls << v.collect { |sd| sd.table_name }
  #  end
  #  tbls.flatten!.uniq!
  #  tbls.collect { |t| Table.find(@server, @name, t) }
  #end

  def initialize(server,db,stats)
    @server=server
    @name=db
    @stats=stats
  end
end
