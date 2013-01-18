# nagios.rb
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
require 'action_view'
require 'text/reform'

module TTT
  class NagiosFormatter < Formatter
    runner_for :nagios
    include ActionView::Helpers::DateHelper

    OK=0
    WARNING=1
    CRITICAL=2
    UNKNOWN=3

    def format(rows, *args)
      options=args.extract_options!
      if !cfg.key? "formatter_options" and !cfg["formatter_options"].key? "nagios"
        stream.puts "Must specify nagios formatter options in the config file."
        return UNKNOWN
      end
      do_alert=false
      alert_level=
        case cfg["formatter_options"]["nagios"]["alert_level"] 
          when "critical"
            CRITICAL
          when "warning"
            WARNING
          when "unknown"
            UNKNOWN
          when "ok"
            OK
          else
            WARNING
        end
      tables=cfg["formatter_options"]["nagios"]["tables"] ? cfg["formatter_options"]["nagios"]["tables"] : []
      real_rows=[]
      output_str=""
      if options[:raw]==true
        real_rows=rows
      else
        real_rows=reject_ignores(rows)
      end
      real_rows.each do |row|
        if [:changed, :new, :deleted, :unreachable].include? row.status
          sst=[row.server, row.database_name, row.table_name].join(".")
          row_alert=false
          tables.each do |rex|
            if Regexp.new(rex).match(sst)
              do_alert=true
              row_alert=true
            end
          end
          if row_alert
            output_str += "#{row.database_name}.#{row.table_name}(#{row.status.to_s.upcase} #{time_ago_in_words row.run_time} ago), "
          end
        end
      end
      if output_str != ""
        puts output_str.gsub!(/, $/,'')
        alert_level if do_alert
      else
        puts "No changes."
        OK
      end
    end
  end
end
