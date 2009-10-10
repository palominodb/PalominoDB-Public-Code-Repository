require 'rubygems'
require 'activerecord'
require 'ttt/format/text'


module TTT
  class EmailFormatter < Formatter
    runner_for :email
    def format(rows, *args)
      TextFormatter.new(stream, cfg).format(rows, args)
    end
  end
end
