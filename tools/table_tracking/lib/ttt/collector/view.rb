require 'rubygems'
require 'ttt/db'
require 'ttt/formatters'
require 'ttt/table_view'

module TTT
  class ViewCollector < Collector
    collect_for :view, "view syntax tracking" do |host,cfg,runtime|
      begin
        TTT::TABLE.all.each do |tbl|
          next if tbl.TABLE_TYPE != "VIEW"
          TTT::TableView.record_timestamps = false
          newtbl = TTT::TableView.new(
            :server => host,
            :database_name => tbl.TABLE_SCHEMA,
            :table_name => tbl.TABLE_NAME,
            :create_syntax => tbl.create_syntax,
            :run_time => runtime
            # Views do not hold creation or update times, so we rely 
            # on our runtime to determine when it first appeared
          )
          oldtbl = TTT::TableView.find_last_by_server_and_database_name_and_table_name(host, tbl.TABLE_SCHEMA, tbl.TABLE_NAME)
          if oldtbl.nil? or oldtbl.create_syntax.nil? then
            newtbl.save
            say "[new]: server:#{host} database:#{newtbl.database_name} view:#{newtbl.table_name}"
          elsif newtbl.create_syntax != oldtbl.create_syntax then
            newtbl.save
            say "[changed]: server:#{host} database:#{newtbl.database_name} view:#{newtbl.table_name}"
          end
          TTT::TableView.record_timestamps = true
        end
      #end # TTT::TABLE.all

      # Dropped table detection
      TTT::TableView.find_most_recent_versions(:conditions => ['server = ?', host]).each do |tbl|
        g=TTT::TABLE.find_by_TABLE_SCHEMA_and_TABLE_NAME(tbl.database_name, tbl.table_name)
        if g.nil? and !tbl.deleted? then
          TTT::TableView.record_timestamps = false
          TTT::TableView.new(
            :server => host,
            :database_name => tbl.database_name,
            :table_name  => tbl.table_name,
            :create_syntax => nil,
            :run_time => runtime
          ).save
          TTT::TableView.record_timestamps = true
          say "[deleted]: server:#{host} database:#{tbl.database_name} table:#{tbl.table_name}"
        end
      end
    rescue Mysql::Error => mye
      if [MYSQL_HOST_NOT_PRIVILEGED, MYSQL_CONNECT_ERROR, MYSQL_TOO_MANY_CONNECTIONS].include? mye.errno 
        say "[unreachable]: server:#{host}"
        TTT::TableView.record_timestamps = false
        prev=TTT::TableView.find_last_by_server(host)
        if prev.nil? or !prev.unreachable?
          TTT::TableView.new(
            :server => host,
            :database_name => nil,
            :table_name  => nil,
            :create_syntax => nil,
            :run_time => runtime
          ).save
        end
        TTT::TableView.record_timestamps = true
      else
        raise mye
      end
    end
  end
  end
  Formatter.for :view, :text do |stream,frm,data,options|
    col_width=frm.page_width/data.attribute_names.length
    unless options[:header]
      stream.puts frm.format(
        # status        server    db_name   tbl_name  created
        "<<<<<<<<<<<< #{'<'*col_width} #{'<'*col_width} #{'<'*col_width} #{'<'*30}",
        data.status, data.server, data.database_name, data.table_name, data.run_time)
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

end
