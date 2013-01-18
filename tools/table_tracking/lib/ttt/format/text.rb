# text.rb
# Copyright (C) 2009-2013 PalominoDB, Inc.
# 
# You may contact the maintainers at eng@palominodb.com.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

require 'rubygems'
require 'active_record'
require 'text/reform'

module TTT
  class TextFormatter < Formatter
    runner_for :text
    def format(rows, *args)
      options=args.extract_options!
      runtime=nil
      rf=Text::Reform.new
      rf.break = Text::Reform.break_at(' ')
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
          stream.puts rf.format('-- '+'<'*27 + '-'*(rf.page_width-26 > 120 ? 120 : rf.page_width-26 ), runtime.to_s)
          self.class.get_formatter_for(row.class.collector).call(stream,rf,row, options.merge(:header=>true))
        end
        self.class.get_formatter_for(row.class.collector).call(stream,rf,row, options)
      end
      true
    end

  end
end
