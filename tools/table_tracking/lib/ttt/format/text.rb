require 'rubygems'
require 'activerecord'
require 'text/reform'

module TTT
  class TextFormatter < Formatter
    runner_for :text
    def format(rows, *args)
      options=args.extract_options!
      runtime=nil
      rf=Text::Reform.new
      rf.page_width=options[:display_width] || 80
      real_rows=[]
      if options[:raw]==true
        real_rows=rows
      else
        real_rows=reject_ignores(rows)
      end
      real_rows.each do |row|
        if row.run_time!=runtime
          stream.puts "" unless runtime.nil?
          runtime=row.run_time
          stream.puts rf.format('-- '+'<'*27 + '-'*(rf.page_width-26), runtime.to_s)
          Formatter.get_formatter_for(row.class.collector, :text).call(stream,rf,row, options.merge(:header=>true))
        end
        Formatter.get_formatter_for(row.class.collector, :text).call(stream,rf,row, options)
      end
      true
    end

  end
end
