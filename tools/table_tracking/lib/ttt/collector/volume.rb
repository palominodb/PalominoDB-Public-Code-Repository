require 'rubygems'
require 'ttt/db'
require 'ttt/formatters'
require 'ttt/table_volume'
require 'ttt/server'

module TTT
  class VolumeCollector < Collector
    collect_for :volume, "table, index, and free size tracking" do |cr,host,cfg,runtime|
      ids=[]
      begin
        srv=TTT::Server.find_or_create_by_name(host)
        srv.cached_size=0
        dbs={}
        TTT::TABLE.all.each do |tbl|
          next if tbl.system_table?
          unless dbs.key? tbl.TABLE_SCHEMA
            dbs[tbl.TABLE_SCHEMA]=srv.schemas.find_or_create_by_name(tbl.TABLE_SCHEMA)
            dbs[tbl.TABLE_SCHEMA].cached_size=0
          end
          dbs[tbl.TABLE_SCHEMA].tables.find_or_create_by_name(tbl.TABLE_NAME)
          datafree=nil
          if tbl.TABLE_COMMENT =~ /InnoDB free: (\d+)/
            datafree=($1.to_i)*1024
          else
            datafree=tbl.DATA_FREE
          end
          tv=TTT::TableVolume.new(
            :server => host,
            :database_name => tbl.TABLE_SCHEMA,
            :table_name => tbl.TABLE_NAME,
            :run_time => runtime,
            :data_length => tbl.DATA_LENGTH,
            :data_free => datafree,
            :index_length => tbl.INDEX_LENGTH
          )
          tv.save
          ids<<tv.id
          say "[volume] server:#{host} schema:#{tbl.TABLE_SCHEMA} table:#{tbl.TABLE_NAME} data_length:#{tbl.DATA_LENGTH} index_length:#{tbl.INDEX_LENGTH}" if(verbose)
          srv.cached_size += tv.size
          dbs[tbl.TABLE_SCHEMA].cached_size += tv.size
        end # Table.all
        srv.save
        pp dbs
        dbs.each_value { |d| d.save }

        # Dropped table detection
        TTT::TableVolume.find_most_recent_versions(:conditions => ['server = ?', host]).each do |tbl|
          g=TTT::TABLE.find_by_TABLE_SCHEMA_and_TABLE_NAME(tbl.database_name, tbl.table_name)
          if g.nil? and !tbl.deleted? then
            TTT::TableVolume.record_timestamps = false
            t=TTT::TableVolume.new(
              :server => host,
              :database_name => tbl.database_name,
              :table_name => tbl.table_name,
              :data_length => nil,
              :data_free => nil,
              :index_length => nil,
              :run_time => runtime
            )
            ids<<t.id
            t.save
            TTT::TableVolume.record_timestamps = true
            say "[deleted]: server:#{host} schema:#{tbl.database_name} table:#{tbl.database_name}"
          elsif g.nil? and tbl.deleted? then
            ids<<tbl.id
          end
        end

      rescue Mysql::Error => mye
        if [MYSQL_HOST_NOT_PRIVILEGED, MYSQL_CONNECT_ERROR, MYSQL_TOO_MANY_CONNECTIONS].include? mye.errno
          say "[unreachable]: server:#{host}"
          TTT::TableVolume.record_timestamps = false
          prev=TTT::TableVolume.find_last_by_server(host)
          if prev.nil? or !prev.unreachable?
            TTT::TableVolume.new(
              :server => host,
              :database_name => nil,
              :table_name => nil,
              :data_length => nil,
              :data_free => nil,
              :index_length => nil,
              :run_time => runtime
            ).save
          end
          TTT::TableVolume.record_timestamps = true
        else
          raise mye
        end
      end
      ids
    end
  end
  Formatter.for :volume, :text do |stream,frm,data,options|
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
end
