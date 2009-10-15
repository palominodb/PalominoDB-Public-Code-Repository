require 'rubygems'
require 'open-uri'
require 'yaml'

module Pdb
  # Thrown when a DSN does not make sense.
  # Constants:
  #   - Unknown (unknown semantics error)
  #   - UnknownCluster (server definition mentions unknown/missing cluster)
  #   - ClusterMismatch (server and cluster do not agree on participation)
  class SemanticsError < Exception
    attr :type
    Unknown = :Unknown
    UnknownCluster = :UnknownCluster
    ClusterMismatch = :ClusterMismatch
    EmptyDSN = :ClusterMismatch
    
    def initialize(type=Unknown)
      @type=type
    end
  end

  # Converts various strings into true/false.
  def self.truth(str)
    trues  = [ "y", "t", "true", "yes" ]
    falses = [ "n", "f", "false", "no" ]
    if trues.include? str
      return true
    elsif falses.include? str
      return false
    end
  end

  # An instance of a DSN.
  # == Examples
  #   # Load a DSN over HTTP
  #   dsn=Pdb::DSN.new("http://dsn.example.com/dsn.yml")
  #   # Load with a regular path
  #   dsn=Pdb::DSN.new("/etc/dsn.yml")
  #   # Load with a valid uri path
  #   dsn=Pdb::DSN.new("file:///opt/pdb/dsn.yml")
  #
  #   # Returns true, or raises an exception.
  #   dsn.validate
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

    # Open a uri as a dsn.
    def open(uri)
      @uri = uri
      @raw = YAML.load(Kernel.open(uri))
    end

    # Initialize a DSN from a hash.
    # It's __HIGHLY__ recommended to run validate after calling this.
    def from_hash(hsh)
      if hsh.class != Hash
        raise ArgumentError, "Provided argument must be a hash."
      end
      @raw=hsh
    end

    # Reloads the dsn from the source uri.
    # This is fundamentally a distructive operation.
    def reload!
      self.open(@uri)
    end

    # Validates a DSN as being 'syntatically' and semantically correct.
    # Syntax errors are thrown if required keys are missing from the dsn.
    # A SemanticsError is thrown if there is disagreement in the DSN.
    # Presently, that means missing clusters, or disagreement between servers and clusters.
    def validate
      if @raw.nil?
        raise SemanticsError.new(SemanticsError::EmptyDSN), "Can not validate an empty dsn."
      end
      host_keys = [ "version", "active", "readfor", "writefor" ]
      cluster_keys = [ "active", "servers", "schemas" ]
      @raw["servers"].each do |srv,d|
        host_keys.each do |k|
          if !d.has_key? k
            raise SyntaxError.new(), "Server '#{srv}' missing required key '#{k}'"
          end
        end
      end
      @raw["clusters"].each do |clu,d|
        cluster_keys.each do |k|
          if !d.has_key? k
            raise SyntaxError.new() "Cluster '#{clu}' missing required key '#{k}'"
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

    # Retrieve destinations for writes for a cluster.
    # How writes are load-balaned is application dependent.
    # This method will only return active hosts.
    def get_write_hosts(cluster)
      write_hosts=[]
      @raw["servers"].each do |srv, d|
        if d["writefor"].class == Array
          write_hosts << srv if d["writefor"].include? cluster and host_active? srv
        elsif d["writefor"].class == String
          write_hosts << srv if d["writefor"] == cluster and host_active? srv
        end
      end
      write_hosts
    end

    # Retrieve read hosts for a cluster.
    # Read load-balancing is application specific, but in general,
    # round-robin, or random selection is better than hammering the
    # first one in the list.
    def get_read_hosts(cluster)
      read_hosts=[]
      @raw["servers"].each do |srv, d|
        if d["readfor"].class == Array
          read_hosts << srv if d["readfor"].include? cluster and host_active? srv
        elsif d["readfor"].class == String
          write_hosts << srv if d["readfor"] == cluster and host_active? srv
        end
      end
      read_hosts
    end

    # Returns names of all the hosts defined.
    def get_all_hosts
      @raw["servers"].keys
    end

    # Returns true or false depending on whether or not the
    # host is active. If there is no such host, 'nil' is returned.
    def host_active?(server)
      if @raw["servers"].has_key? server
        Pdb.truth @raw["servers"][server]["active"]
      else
        nil
      end
    end

    # Same as above, but with a cluster.
    def cluster_active?(cluster)
      if @raw["clusters"].has_key? cluster
        Pdb.truth @raw["clusters"][cluster]["active"]
      else
        nil
      end
    end

    # Gets the version of mysql running on a host.
    def get_version(server)
      @raw["servers"][server]["version"]
    end
  end
end
