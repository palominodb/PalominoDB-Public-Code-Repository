require 'ipaddr'

class SqlProfilerHost < SqlProfiler
  set_primary_keys :checksum, :host
  self.inheritance_column = nil

  def find_by_host(host)
    self.class.find(:last, :select => 'host,ip,first_seen,last_seen', :conditions => [%Q{host = ?}, host])
  end
  def ip
    IPAddr.new(read_attribute('ip'), Socket::AF_INET).to_s
  end

  def queries(reviewed=false)
    SqlProfilerQuery.find(:all, :joins => 'INNER JOIN sql_profiler_hosts ON sql_profiler_hosts.checksum=sql_profiler_queries.checksum',
                          :conditions => [%Q{host = ? AND type = 'DESTINATION'} + (reviewed ? "" : ' AND reviewed_by IS NULL'), host])
  end
end
