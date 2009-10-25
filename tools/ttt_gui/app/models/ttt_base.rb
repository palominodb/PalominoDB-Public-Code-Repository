require 'ttt'
require 'ttt/collector'
class TttBase
  attr_reader :name
  attr_reader :stats

  def self.find(name)
    raise RuntimeError, "Abstract base."
  end

  def self.all
    raise RuntimeError, "Abstract base."
  end

  private
  def initialize(name, stats)
    raise RuntimeError, "Abstract (and private!)!!"
  end
end
