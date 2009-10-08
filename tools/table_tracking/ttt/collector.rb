require 'rubygems'
require 'ttt/db'
require 'pp'

module TTT
  class Collector
    MYSQL_CONNECT_ERROR = 2003
    Runtime = Time.now
    @@collectors = {}
    @@verbose = true
    def self.register(name)
      say "registering: #{name}(#{self.name})"
      @@collectors[name] = self
    end
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

    def self.verbose
      @@verbose
    end

    def self.verbose=(verb)
      @@verbose=verb
    end

    def self.load_all
      Dir.glob( File.dirname(__FILE__) + "/collector/*" ).each do |col|
        Kernel.load col
      end
    end
  end
end
