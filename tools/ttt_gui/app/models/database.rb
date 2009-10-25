require 'ttt'
require 'ttt/collector'
class Database
  attr_reader :server
  attr_reader :name
  attr_reader :stats
  def self.find(server,name)
    stats={}
    TTT::TrackingTable.tables.each do |s,k|
      stats[s]=k.find(:all, :conditions => ["server = ? and database_name = ? and run_time = ?", server, name, TTT::Collector.get_last_run(s)])
    end
    self.new(server,name, stats)
  end
  def self.all
    dbs=[]
    stats={}
    TTT::TrackingTable.tables.each do |s,k|
      stats[s]=k.find(:all, :conditions => ["server = ? and database_name = ? and run_time = ?", server, name, TTT::Collector.get_last_run(s)])
    end

    #TTT::TrackingTable.tables.each do |s,tt|
    #  dbs << tt.find(:all, :select => "server, database_name, sum(data_length) as data_length, sum(index_length) as index_length", :group => 'server, database_name', :conditions => ["run_time = ?", TTT::Collector.get_last_run(s)]).map { |s|
    #    s.database_name
    #  }
    #end

    dbs.flatten.uniq
  end

  def self.get_size(server,database)
    TTT::TableVolume.aggregate_by_schema(server,database)
  end

  def get_size
    size=0
    @stats[:volume].each do |d|
      size+=d.size
    end
    size
  end

  def tables
    tbls=[]
    @stats.collect do |k,v|
      tbls << v.collect { |sd| sd.table_name }
    end
    tbls.flatten!.uniq!
    tbls.collect { |t| Table.find(@server, @name, t) }
  end

  def initialize(server,db,stats)
    @server=server
    @name=db
    @stats=stats
  end
end
