require 'rubygems'
require 'ttt/db'
require 'pp'

module TTT
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
    Runtime = Time.now
    @@collectors = {}
    @@verbose = true

    # Called by subclasses of Collector to, well, register themsevles
    # as a valid collector.
    def self.register(name)
      say "registering: #{name}(#{self.name})"
      @@collectors[name] = self
    end

    # Abstract method to be reimplemented by subclasses.
    def self.collect(host,cfg)
      raise NotImplementedError, "This is an abstract class."
    end

    def self.each
      @@collectors.each_value { |x| yield(x) }
    end

    def self.[](x)
      @@collectors[x]
    end

    def self.say(text="")
      puts(text) if @@verbose
    end

    # Returns if collectors should be verbose, or not.
    def self.verbose
      @@verbose
    end

    # Set if collectors should be verbose.
    def self.verbose=(verb)
      @@verbose=verb
    end

    # Loads all collectors under: <gems path>/table-tracking-toolkit-<version>/lib/ttt/collector/*
    # This must be called before collectors will function.
    def self.load_all
      Dir.glob( File.dirname(__FILE__) + "/collector/*" ).each do |col|
        Kernel.load col
      end
    end
  end
end
