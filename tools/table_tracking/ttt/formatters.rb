require 'rubygems'

module TTT
  class Formatter
    attr :stream
    def initialize(stream)
      @stream=stream
    end
    def format(rows)
      raise Exception, "Use a real formatter."
    end

  end
end
