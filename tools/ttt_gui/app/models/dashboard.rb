class Dashboard
  @@dashboards = {}
  def self.dashboard_for(name)
    @@dashboards[name] = self
  end
  def self.all
    @@dashboards
  end

  def self.[x]
    @@dashboards[x]
  end

end

class VolumeDashboard < Dashboard
  dashboard_for :volume
  def initialize(view=:top, *args)
    if ![:detail, :top].include? view
      raise ArgumentError, "Must be :detail or :top"
    end
  end
  def last_change
    TTT::Collector.get_last_run(:volume)
  end

  def top_5

  end
end
