# definition.rb
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

TTT::Collector.new(TTT::TableDefinition, "'create syntax' tracking") do |rd|
  rd.tables.each do |t|
    next if t.system_table?
    newt = rd.stat.new(
      :server => rd.host,
      :database_name => t.schema,
      :table_name => t.name,
      :create_syntax => t.create_syntax,
      :created_at => t.create_time,
      :run_time => rd.runtime,
      :updated_at => t.update_time
    )
    # Remove AUTO_INCREMENT options from the create syntax per
    # ticket [9babce26e5e802dbc14737404cb73d84d605ef71]
    # CREATE TABLE foo ( ... ) ENGINE=InnoDB AUTO_INCREMENT=7946150 DEFAULT CHARSET=utf8
    newt.create_syntax.gsub!(/AUTO_INCREMENT=\d+\s+/, "")
    oldt = rd.stat.find_last_by_table(rd.host, t)
    if oldt.nil? or oldt.create_syntax.nil? then
      newt.save
      rd << newt.id
      rd.logger.info "[new] #{newt.inspect}"
    elsif newt.create_syntax != oldt.create_syntax then
      newt.save
      rd.stat_updated(newt.id, oldt.id)
      rd.logger.info "[changed] #{oldt.inspect}"
    end
  end
  rd.get_prev_version.each do |t|
    rd.logger.info "[delete-check] #{t.database_name}.#{t.table_name}"
    g=rd.tables.find_by_schema_and_table(t.database_name, t.table_name)
    if g.nil? and !t.deleted? then
      tbl=rd.stat.new(
        :server => rd.host,
        :database_name => t.database_name,
        :table_name => t.table_name,
        :create_syntax => nil,
        :run_time => rd.runtime,
        :created_at => t.created_at,
        :updated_at => '0000-00-00 00:00:00'
      )
      tbl.save
      rd.stat_updated(tbl.id, t.id)
      rd.logger.info "[deleted] #{t.inspect}"
    end
  end
  rd
end

TTT::Formatter.for :definition, :text do |stream,frm,data,options|
  col_width=frm.page_width/data.attribute_names.length
  unless options[:header]
    stream.puts frm.format(
      # status        server    db_name   tbl_name  created
      "<<<<<<<<<<<< #{'<'*col_width} #{'<'*col_width} #{'<'*col_width} #{'<'*30}",
      data.status, data.server, data.database_name, data.table_name, data.created_at)
      if(options[:full])
        # This is needed because format CONSUMES strings, and matching
        # doesn't work when 'database_name' et. al. are empty!
        data.reload
        stream.puts frm.format(
          "OLD" + ' '*(frm.page_width/2) + "NEW",
          "["*(frm.page_width/2) + "]]" + ' ' "[[" + "]"*(frm.page_width/2),
          data.previous_version.nil? ? nil : data.previous_version.create_syntax, data.create_syntax)
      end
  else
    stream.puts frm.format(
      "<<<<<<<<<<<< #{'<'*col_width} #{'<'*col_width} #{'<'*col_width} #{'<'*30}",
      "status", "server", "database name", "table name", "created at")
  end
end

