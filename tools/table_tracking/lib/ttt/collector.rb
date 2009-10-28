require 'rubygems'
require 'ttt/db'
require 'ttt/information_schema'
require 'ttt/history'
require 'pp'

module TTT
  class CollectorRunningError < Exception; end
  # Base class for all collectors.
  # A collector is actually a set of classes.
  # A class derived from ActiveRecord::Base (such as TableDefinition)
  # and a class derived from Collector (such as DefinitionCollector)
  # ttt-collect will then run each collector's 'collect' method
  # NOTE: There is no guaranteed order in which collectors are called.
  #       Do not depend on any particular order.
  # See: DefinitionCollector and VolumeCollector for examples.
  class Collector
    # Errno returned by Mysql when it cannot connect.
    MYSQL_CONNECT_ERROR = 2003
    MYSQL_TOO_MANY_CONNECTIONS = 1040
    MYSQL_HOST_NOT_PRIVILEGED = 1130
    #Runtime = Time.now
    @@collectors = {}
    @@verbose = true
    @@debug = false 
    @@loaded_collectors = false

    class_inheritable_reader :stat, :desc, :run

    # Called by subclasses of Collector to, well, register themsevles
    # as a valid collector.
    def self.collect_for(name, desc="" )
      yell "collecter for: #{name}(#{self.name})"
      @@collectors[name] = self
      write_inheritable_attribute :stat, name
      write_inheritable_attribute :desc, desc
      write_inheritable_attribute :run, Proc.new
    end

    def self.collect_hosts(hosts, cfg, runtime=Time.now)
      runs=[]
      hosts.each do |h|
        runs<<self.collect(h,cfg,runtime)
      end
      runs
    end

    def self.collect(host,cfg,runtime=Time.now)
      #raise NotImplementedError, "This is an abstract class."
      CollectorRun.transaction do
        r=CollectorRun.find_or_create_by_collector(stat.to_s)
        if(cfg["ttt_connection"]["adapter"] == "mysql")
          if(CollectorRun.connection.select_value("SELECT IS_FREE_LOCK('ttt.collector.#{stat.to_s}')").to_i == 1)
            CollectorRun.connection.execute("SELECT GET_LOCK('ttt.collector.#{stat.to_s}',0.25)")
          else
            raise CollectorRunningError, "Only one collector per statistic may run at a time."
          end
        end
        r.lock!
        TTT::InformationSchema.connect(host, cfg)
        r.run_ids=self.run[r,host,cfg,runtime]
        r.last_run=runtime
        r.save
        if cfg['ttt_connection']['adapter'] == "mysql" then
          CollectorRun.connection.execute("SELECT RELEASE_LOCK('ttt.collector.#{stat.to_s}')")
        end
        r
      end
    end

    def self.get_last_run(stat=self.stat)
      CollectorRun.find_by_collector(stat.to_s).reload.last_run
    end

    def self.each
      @@collectors.each_value { |x| yield(x) }
      true
    end

    def self.collectors
      @@collectors.values
    end

    def self.[](x)
      @@collectors[x]
    end

    def self.say(text="")
      puts(text) if @@verbose
    end

    def self.yell(text="")
      puts(text) if @@debug
    end

    # Returns if collectors should be verbose, or not.
    def self.verbose
      @@verbose
    end

    def self.debug
      @@debug
    end

    # Set if collectors should be verbose.
    def self.verbose=(verb)
      @@verbose=verb
    end

    def self.debug=(deb)
      @@debug=deb
    end

    # Loads all collectors under: <gems path>/table-tracking-toolkit-<version>/lib/ttt/collector/*
    # This must be called before collectors will function.
    def self.load_all
      unless @@loaded_collectors
        Dir.glob( File.dirname(__FILE__) + "/collector/*" ).each do |col|
          Kernel.load col
        end
        @@loaded_collectors=true
      end
    end
  end

  class CollectorRun < ActiveRecord::Base
    has_many :snapshots, :class_name => 'TTT::Snapshot'
    def run_ids=(ids)
      @run_ids=ids
    end
    def run_ids
      @run_ids
    end
  end
end
