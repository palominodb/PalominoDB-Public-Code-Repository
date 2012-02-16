
class SqlProfilerHistory < SqlProfiler
  set_primary_keys :checksum, :ts_min, :ts_max
end
