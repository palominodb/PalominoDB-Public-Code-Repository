require 'rubygems'
require 'ttt/db'
require 'ttt/formatters'
require 'ttt/table_definition'

#def self.collect(host,cfg)
module TTT
  class DefinitionCollector < Collector
    collect_for :definition, "'create syntax' tracking" do |cr,host,cfg,runtime|
      ids=(TTT::TableView.find_most_recent_versions({:conditions => ['server = ?', host]}).collect { |v| v.id } ).to_set
      begin
        srv=TTT::Server.find_or_create_by_name(host)

        TTT::TABLE.all.each do |tbl|
          next if tbl.system_table?
          srv.schemas.find_or_create_by_name(tbl.TABLE_SCHEMA).tables.find_or_create_by_name(tbl.TABLE_NAME)
          TTT::TableDefinition.record_timestamps = false
          newtbl = TTT::TableDefinition.new(
            :server => host,
            :database_name => tbl.TABLE_SCHEMA,
            :table_name => tbl.TABLE_NAME,
            :create_syntax => tbl.create_syntax,
            :created_at => tbl.CREATE_TIME,
            :run_time => runtime,
            :updated_at => tbl.UPDATE_TIME) # InnoDB tables do not have this flag, so it will be null when the table is InnoDB.
            oldtbl = TTT::TableDefinition.find_last_by_server_and_database_name_and_table_name(host, tbl.TABLE_SCHEMA, tbl.TABLE_NAME)
            if oldtbl.nil? or oldtbl.create_syntax.nil? then
              newtbl.save
              ids<<newtbl.id
              say "[new]: server:#{host} database:#{newtbl.database_name} table:#{newtbl.table_name}"
            elsif newtbl.created_at != oldtbl.created_at then
              newtbl.save
              ids<<[newtbl.id,oldtbl.id]
              ids.delete oldtbl.id
              say "[changed]: server:#{host} database:#{newtbl.database_name} table:#{newtbl.table_name}"
            end
            TTT::TableDefinition.record_timestamps = true
        end

        # Dropped table detection
        TTT::TableDefinition.find_most_recent_versions(:conditions => ['server = ?', host]).each do |tbl|
          g=TTT::TABLE.find_by_TABLE_SCHEMA_and_TABLE_NAME(tbl.database_name, tbl.table_name)
          if g.nil? and !tbl.deleted? then
            TTT::TableDefinition.record_timestamps = false
            t=TTT::TableDefinition.new(
              :server => host,
              :database_name => tbl.database_name,
              :table_name  => tbl.table_name,
              :create_syntax => nil,
              :run_time => runtime,
              :created_at => tbl.created_at,
              :updated_at => "0000-00-00 00:00:00"
            )
            t.save
            ids<<[t.id, tbl.id]
            ids.delete tbl.id
            TTT::TableDefinition.record_timestamps = true
            say "[deleted]: server:#{host} database:#{tbl.database_name} table:#{tbl.table_name}"
          end
        end
      rescue Mysql::Error => mye
        if [MYSQL_HOST_NOT_PRIVILEGED, MYSQL_CONNECT_ERROR, MYSQL_TOO_MANY_CONNECTIONS].include? mye.errno 
          say "[unreachable]: server:#{host}"
          TTT::TableDefinition.record_timestamps = false
          prev=TTT::TableDefinition.find_last_by_server(host)
          if prev.nil? or !prev.unreachable?
            t=TTT::TableDefinition.new(
              :server => host,
              :database_name => nil,
              :table_name  => nil,
              :create_syntax => nil,
              :run_time => runtime,
              :created_at => "0000-00-00 00:00:00",
              :updated_at => "0000-00-00 00:00:00"
            )
            t.save
            ids<<t.id
          end
          TTT::TableDefinition.record_timestamps = true
        else
          raise mye
        end
      end
      if ids != (TTT::TableView.find_most_recent_versions({:conditions => ['server = ?', host]}).collect { |v| v.id } ).to_set
        ids.to_a
      else
        []
      end
    end
  end
  Formatter.for :definition, :text do |stream,frm,data,options|
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
end
