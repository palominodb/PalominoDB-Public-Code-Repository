require 'ttt'
require 'ttt/collector'
class Server 
  attr_reader :stats
  attr_reader :name
  def self.find(name)
    stats={}
    TTT::TrackingTable.tables.each do |s,k|
      stats[s]=k.find(:all, :conditions => ["server = ? and run_time = ?", name, TTT::Collector.get_last_run(s)])
    end
    self.new(name, stats)
  end
  def self.servers
    servers=[]
    TTT::TrackingTable.tables.each do |s,tt|
      servers << tt.find_most_recent_versions( :select => "server", :group => :server ).map { |s| s.server }
      #servers << tt.find(:all, :select => "server", :group => [:server], :conditions => ["run_time = ?", TTT::Collector.get_last_run(s)]).map { |s| s.server }
    end
    servers.flatten.uniq
  end
  def self.all
    self.servers
  end
  def self.get_size(server)
    TTT::TableVolume.aggregate_by_server(server)
  end

  def get_size
    size=0
    @stats[:volume].each do |v|
      size+=v.size
    end
    size
  end


  def self.each
    self.all.each { |s| yield(self.find(s)) }
  end

  def databases
    dbs=[]
    @stats.collect do |k,v|
      dbs << v.collect { |s| s.database_name }
    end
    #dbs.flatten.uniq #.collect { |d| Database.find(name, d) }
    dbs.flatten!.uniq!
    dbs.collect { |d| Database.find(name, d) }
  end

  def database(dname)
    Database.find(name, dname)
  end

  private

  def initialize(name,stats)
    @name=name
    @stats=stats
  end


end
