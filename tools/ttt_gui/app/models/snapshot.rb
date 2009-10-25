include 'ttt/db'
include 'ttt/collector'
include 'ttt/table_view'
include 'ttt/table_definition'
include 'ttt/table_volume'

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
