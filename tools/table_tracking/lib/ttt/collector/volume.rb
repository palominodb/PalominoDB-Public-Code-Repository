require 'rubygems'
require 'ttt/db'
require 'ttt/formatters'
require 'ttt/table_volume'
require 'ttt/server'

TTT::Collector.new(TTT::TableVolume, "table, index, and free size tracking") do |rd|
  srv=TTT::Server.find_by_name(rd.host)
  srv.cached_size=0
  dbs={}
  rd.snapshot.clear
  rd.tables.each do |t|
    next if t.system_table?
    unless dbs.key? t.schema
      dbs[t.schema]=srv.schemas.find_by_name(t.schema)
      dbs[t.schema].cached_size=0
    end
    datafree=nil
    if t.comment =~ /InnoDB free: (\d+)/
      datafree=($1.to_i)*1024
    else
      datafree=t.data_free
    end
    tv=rd.stat.new(
      :server => rd.host,
      :database_name => t.schema,
      :table_name => t.name,
      :run_time => rd.runtime,
      :data_length => t.data_length,
      :data_free => datafree,
      :index_length => t.index_length
    )
    tv.save
    rd<<tv.id
    rd.logger.info "[volume] server:#{rd.host} schema:#{t.schema} table:#{t.name} data_length:#{t.data_length} index_length:#{t.index_length}"
    srv.cached_size += tv.size
    dbs[tv.database_name].cached_size += tv.size
  end # All tables
  srv.save
  dbs.each_value { |d| d.save }

  ActiveRecord::Base.benchmark("Delete Checks") {
  rd.get_prev_version.each do |t|
    rd.logger.info "[delete-check] #{t.database_name}.#{t.table_name}"
    g=rd.tables.find_by_schema_and_table(t.database_name, t.table_name)
    if g.nil? and !t.deleted? then
      tbl=rd.stat.new(
        :server => rd.host,
        :database_name => t.database_name,
        :table_name => t.table_name,
        :data_length => nil,
        :data_free => nil,
        :index_length => nil,
        :run_time => rd.runtime
      )
      tbl.save
      rd<<tbl.id
      rd.logger.info "[deleted] #{t.inspect}"
    end
  end
  }
  rd
end

TTT::Formatter.for :volume, :text do |stream,frm,data,options|
  col_width=frm.page_width/(options[:full] ? 6 : 5)
  unless options[:header]
    if options[:full]
      stream.puts frm.format(
        # status        server           db_name          tbl_name         data_len  index_len data_free
        "<<<<<<<<<<< #{'<'*col_width} #{'<'*col_width} #{'<'*col_width} #{'<'*18} #{'<'*18} #{'<'*18}",
        data.status, data.server, data.database_name, data.table_name,
        data.data_length.nil? ? nil : data.data_length/1024/1024,
        data.index_length.nil? ? nil : data.index_length/1024/1024,
        data.data_free.nil? ? nil : data.data_free/1024/1024)
    else
      stream.puts frm.format(
        # status        server    db_name   tbl_name  size
        "<<<<<<<<<<< #{'<'*col_width} #{'<'*col_width} #{'<'*col_width} #{'<'*18}",
        data.status, data.server, data.database_name, data.table_name,
        data.data_length.nil? ? nil : (data.data_length + data.index_length)/1024/1024)
    end
  else # :header
    if options[:full]
      stream.puts frm.format(
        # status        server           db_name          tbl_name         data_len  index_len data_free
        "<<<<<<<<<<< #{'<'*col_width} #{'<'*col_width} #{'<'*col_width} #{'<'*18} #{'<'*18} #{'<'*18}",
        "status", "server", "database name", "table name", "data length(mb)", "index length(mb)", "data free(mb)")
    else
      stream.puts frm.format(
        "<<<<<<<<<<< #{'<'*col_width} #{'<'*col_width} #{'<'*col_width} #{'<'*18}",
        "status", "server", "database name", "table name", "size (mb)")
    end
  end
end

