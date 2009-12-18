
class SqlProfilerQuery < SqlProfiler
  set_primary_key "checksum"

  def get_history
    SqlProfilerHistory.find_all_by_checksum(id, :order => 'ts_max DESC')
  end

  def get_last_history
    SqlProfilerHistory.find_by_checksum(id, :order => 'ts_max DESC')
  end

  def get_hosts
    SqlProfilerHost.find_all_by_checksum(id)
  end

  def get_source_hosts
    SqlProfilerHost.find_all_by_checksum(id, :conditions => [%Q{type = 'SOURCE'}])
  end
  def get_destination_hosts
    SqlProfilerHost.find_all_by_checksum(id, :conditions => [%Q{type = 'DESTINATION'}])
  end

  def is_source_host?(host)
    get_source_hosts.include? host
  end
  def is_destination_host?(host)
    get_destination_hosts.include? host
  end
end
