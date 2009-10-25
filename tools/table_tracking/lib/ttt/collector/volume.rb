require 'rubygems'
require 'ttt/db'
require 'ttt/formatters'
require 'ttt/table_volume'

module TTT
  class VolumeCollector < Collector
    collect_for :volume, "table, index, and free size tracking" do |host,cfg,runtime|
      begin
        TTT::TABLE.all.each do |tbl|
          next if tbl.system_table?
          datafree=nil
          if tbl.TABLE_COMMENT =~ /InnoDB free: (\d+)/
            datafree=($1.to_i)*1024
          else
            datafree=tbl.DATA_FREE
          end
          TTT::TableVolume.new(
            :server => host,
            :database_name => tbl.TABLE_SCHEMA,
            :table_name => tbl.TABLE_NAME,
            :run_time => runtime,
            :data_length => tbl.DATA_LENGTH,
            :data_free => datafree,
            :index_length => tbl.INDEX_LENGTH
          ).save
          say "[volume] server:#{host} schema:#{tbl.TABLE_SCHEMA} table:#{tbl.TABLE_NAME} data_length:#{tbl.DATA_LENGTH} index_length:#{tbl.INDEX_LENGTH}" if(verbose)
        end # Table.all

        # Dropped table detection
        TTT::TableVolume.find_most_recent_versions(:conditions => ['server = ?', host]).each do |tbl|
          g=TTT::TABLE.find_by_TABLE_SCHEMA_and_TABLE_NAME(tbl.database_name, tbl.table_name)
          if g.nil? and !tbl.deleted? then
            TTT::TableVolume.record_timestamps = false
            TTT::TableVolume.new(
              :server => host,
              :database_name => tbl.database_name,
              :table_name => tbl.table_name,
              :data_length => nil,
              :data_free => nil,
              :index_length => nil,
              :run_time => runtime
            ).save
            TTT::TableVolume.record_timestamps = true
            say "[deleted]: server:#{host} schema:#{tbl.database_name} table:#{tbl.database_name}"
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
