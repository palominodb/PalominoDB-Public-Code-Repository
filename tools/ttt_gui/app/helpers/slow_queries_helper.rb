require 'set'
module SlowQueriesHelper
  begin
    require 'graphviz'
  rescue Exception
  end

  IMAGE_PATH=RAILS_ROOT+"/public/images"
  SLOW_QUERY_PATH="slow_queries"
  FULL_SQ_PATH="#{IMAGE_PATH}/#{SLOW_QUERY_PATH}"

  SQ_CACHE_TIME=30.minutes

  def query_diagram(queries)
    return nil unless Object.const_defined? 'GraphViz'
    query_hosts=SqlProfilerHost.all(:conditions => ["type = 'DESTINATION' AND checksum IN (?)", queries])
    make_query_diagram(query_hosts, queries)
  end

  def host_query_diagram(host, reviewed=false)
    return nil unless Object.const_defined? 'GraphViz'
    make_query_diagram([host], host.queries(reviewed))
  end

  def make_query_diagram(hosts, queries)
    return nil unless Object.const_defined? 'GraphViz'
    sha_file=Digest::SHA1.hexdigest(queries.map {|q| q.id}.join(',')) # Not terribly 'secure', per-se. But, I'm after caching help.
    if File.exist?(FULL_SQ_PATH + "/#{sha_file}.png") and File.stat(FULL_SQ_PATH + "/#{sha_file}.png").mtime < SQ_CACHE_TIME.ago
      logger.debug "Unlinking stale diagram."
      File.unlink FULL_SQ_PATH + "/#{sha_file}.png"
    elsif File.exist?(FULL_SQ_PATH + "/#{sha_file}.png")
      return SLOW_QUERY_PATH + "/#{sha_file}.png"
    end

    g=GraphViz.new :QueryGraph, :type => :digraph#, :use => ''
    g[:bgcolor] = 'transparent'
    g[:overlap] = false
    g[:compound] = true
    g[:rankdir] = "LR"


    seen_hosts=Set.new
    src_hosts={}

    #hosts.each do |qh|
    #  qhstr = (qh.host || qh.ip)
    #  next if seen_hosts.member? qhstr
    #  logger.debug("Doing dest-host: #{qhstr}");
    #  seen_hosts << qhstr
    #  g.add_node("dh#{qhstr.gsub('.', '_')}", :label => qhstr, :style => 'filled', :bgcolor => 'white')
    #end

    queries.each do |q|
      logger.debug("Doing q: #{q.checksum}");
      q.get_source_hosts.map do |shost|
        shstr = (shost.host || shost.ip)
        logger.debug("\tDoing q: #{q.checksum}  src-host: #{shstr}")
        sg=nil
        if src_hosts[shstr]
          sg=src_hosts[shstr]
        else
          logger.debug("\tAdding src-host '#{shstr}' subgraph")
          sg=g.add_graph("cluster0#{shstr.gsub('.','_')}")
          sg[:label] = shstr
          sg[:style] = 'filled'
          sg[:color]='lightgrey'
          src_hosts[shstr]=sg
        end
        n=sg.add_node("q#{q.checksum}_sh#{shstr}",
                       :label => q.fingerprint[0,45] + (q.fingerprint.length >=45 ? "..." : ""),
                       :shape => 'rect')
        hosts.each do |qh|
          next unless q.checksum == qh.checksum
          qhstr=(qh.host || qh.ip)
          logger.debug("\tDoing q: #{q.checksum}  dest-host: #{qhstr}")
          n2=nil
          unless n2=g.get_node("dh#{qhstr.gsub('.','_')}")
            qhstr = (qh.host || qh.ip)
            next if seen_hosts.member? qhstr
            seen_hosts << qhstr
            n2=g.add_node("dh#{qhstr.gsub('.', '_')}", :label => qhstr, :style => 'filled', :bgcolor => 'white')
          end
          g.add_edge(n,n2)
        end
      end
    end

    # Add a subgraph for all source hosts.
    #source_hosts=queries.map { |q| q.get_source_hosts.map { |sh| sh.host || sh.ip } }
    #source_hosts.flatten!
    #source_hosts.uniq!
    #source_hosts.each do |sh|
    #  logger.debug("Doing src-host: #{sh}");
    #  sg=g.add_graph("cluster0#{sh.gsub('.','_')}")
    #  sg[:label] = sh
    #  sg[:style] = 'filled'
    #  sg[:color]='lightgrey'
    #  seen_hosts=Set.new
    #  hosts.each do |qh|
    #    qhstr=(qh.host || qh.ip)
    #    next if seen_hosts.member? qhstr
    #    seen_hosts << qhstr
    #    logger.debug("Doing dest-host: #{qhstr}");
    #    n1=nil
    #    if g.get_node("dh#{qhstr.hash}")
    #      n1=g.get_node("dh#{qhstr.hash}")
    #    else
    #      n1=g.add_node("dh#{qhstr.hash}", :label => qhstr, :style => 'filled', :bgcolor => 'white')
    #    end
    #    queries.each do |q|
    #      logger.debug("Doing q: #{q.checksum}, node: q#{q.checksum}_sh#{sh.hash}");
    #      unless sg.get_node("q#{q.checksum}_sh#{sh.gsub('.', '_')}_dh#{qhstr.gsub('.','_')}")
    #        n2=sg.add_node("q#{q.checksum}_sh#{sh.gsub('.', '_')}_dh#{qhstr.gsub('.','_')}",
    #                       :label => q.fingerprint[0,45] + (q.fingerprint.length >=45 ? "..." : ""),
    #                       :shape => 'rect')
    #        logger.debug("adding edge from '#{q.fingerprint[0,45]}' sh #{sh} to #{qhstr}")
    #        g.add_edge(n2,n1)# if q.checksum == qh.checksum
    #      end
    #    end
    #  end
    #end

    g.output( :png => "#{FULL_SQ_PATH}/#{sha_file}.png" )
    SLOW_QUERY_PATH+"/#{sha_file}.png"
  end

  private
end
