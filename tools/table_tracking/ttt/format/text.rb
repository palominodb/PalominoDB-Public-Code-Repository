require 'rubygems'
require 'pp'

module TTT
  class TextFormatter < Formatter
    def format(rows)
      rows.each { |row| pp row }
    end
  end
end
