require 'rubygems'

module IfoGet
  class Plugin
    attr_reader :global_keys
    def initialize(global_keys)
      @global_keys = global_keys
    end
    def finalize()
    end

    def self.host_keys()
      return []
    end
    def self.group_keys()
      return []
    end
    def self.global_keys()
      return []
    end

    def process(ssh, group, host, group_keys, host_keys)
      return true
    end
  end
end
