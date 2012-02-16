require  'ttt/db'
require  'ttt/collector'
require  'ttt/table_view'
require  'ttt/table_definition'
require  'ttt/table_volume'

class Snapshot
  attr_reader :time
  def self.get
  end
  def self.head
    self.get(:head)
  end
  def previous
  end
  private
  def initialize(at=nil)
    @time=at
  end
end
