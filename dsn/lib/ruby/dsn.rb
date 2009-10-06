require 'rubygems'
require 'open-uri'
require 'yaml'

module Pdb
  class SemanticsError < Exception
    attr :type
    Unknown = :Unknown
    UnknownCluster = :UnknownCluster
    ClusterMismatch = :ClusterMismatch
    
    def initialize(type=Unknown)
      @type=type
    end
  end

  def self.truth(str)
    trues  = [ "y", "t", "true", "yes" ]
    falses = [ "n", "f", "false", "no" ]
    if trues.include? str
      return true
    elsif falses.include? str
      return false
    end
  end
  class DSN
    attr_reader :raw
    def initialize(uri=nil)
      @uri = uri
      if uri.nil?
        @raw = nil
      else
        @raw = YAML.load(Kernel.open(uri))
      end
    end

    def open(uri)
      @uri = uri
      @raw = YAML.load(Kernel.open(uri))
    end

    def reload!
      self.open(@uri)
    end

    def validate
      host_keys = [ "version", "active", "readfor", "writefor" ]
      cluster_keys = [ "active", "servers", "schemas" ]
      @raw["servers"].each do |srv,d|
        host_keys.each do |k|
          if !d.has_key? k
            raise SyntaxError.new "Server '#{srv}' missing required key '#{k}'"
          end
        end
      end
      @raw["clusters"].each do |clu,d|
        cluster_keys.each do |k|
          if !d.has_key? k
            raise SyntaxError.new "Cluster '#{clu}' missing required key '#{k}'"
          end
        end
      end

      @raw["servers"].each do |srv,d|
        d.values_at("writefor", "readfor").flatten.uniq.compact.each do |clu|
          if !@raw["clusters"].has_key? clu
            raise SemanticsError.new(SemanticsError::UnknownCluster), "Server '#{srv}' mentions unknown cluster '#{clu}'"
          end
          if !@raw["clusters"][clu]["servers"].include? srv
            raise SemanticsError.new(SemanticsError::ClusterMismatch), "Server '#{srv}' claims to participate in '#{clu}', but the cluster doesn't agree."
          end
        end
      end
      # XXX: This can break perfectly valid DSN.
      #@raw["clusters"].each do |clu,d|
      #  d["servers"].each do |srv|
      #    if !@raw["servers"][srv].values_at("writefor", "readfor").flatten.uniq.compact.include? clu
      #      raise SemanticsError.new "Cluster '#{clu}' claims to have '#{srv}', but the server doesn't agree."
      #    end
      #  end
      #end
      true
    end

    def get_write_hosts(cluster)
      write_hosts=[]
      @raw["servers"].each do |srv, d|
        write_hosts << srv if d["writefor"] == cluster and host_active? srv
      end
      write_hosts
    end
    def get_read_hosts(cluster)
      read_hosts=[]
      @raw["servers"].each do |srv, d|
        read_hosts << srv if d["readfor"] == cluster and host_active? srv
      end
      read_hosts
    end
    def get_all_hosts
      @raw["servers"].keys
    end
    def host_active?(server)
      if @raw["servers"].has_key? server
        Pdb.truth @raw["servers"][server]["active"]
      else
        nil
      end
    end
    def cluster_active?(cluster)
      if @raw["clusters"].has_key? cluster
        Pdb.truth @raw["clusters"][cluster]["active"]
      else
        nil
      end
    end

    def get_version(server)
      @raw["servers"][server]["version"]
    end
  end
end
