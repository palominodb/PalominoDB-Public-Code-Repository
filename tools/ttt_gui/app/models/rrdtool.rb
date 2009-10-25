class RrdError < Exception ; end
class Rrdtool
  attr_reader :path
  def initialize(path="/usr/bin/rrdtool")
    @path=path
  end
  def exec(params)
    pp params
    res=%x{#{path} #{params} }
    pp res
    if res =~ /^ERROR:(.*)/
      raise RrdError, $1
    end
    res
  end
end
