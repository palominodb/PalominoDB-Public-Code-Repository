require 'rubygems'
require 'ttt/db'
require 'ttt/table_definition'

module TTT
  class DefinitionCollector < Collector
    register "definition"
    def self.collect(host,cfg)
      TTT::InformationSchema.connect(host, cfg)
      begin
        TTT::TABLE.all.each do |tbl|
          next if tbl.TABLE_SCHEMA == "information_schema"
          newtbl = TTT::TableDefinition.new(
            :server => host,
            :database_name => tbl.TABLE_SCHEMA,
            :table_name => tbl.TABLE_NAME,
            :create_syntax => tbl.create_syntax,
            :created_at => tbl.CREATE_TIME,
            :run_time => Runtime,
            :updated_at => tbl.UPDATE_TIME)
            oldtbl = TTT::TableDefinition.find_last_by_server_and_database_name_and_table_name(host, tbl.TABLE_SCHEMA, tbl.TABLE_NAME)
            if oldtbl.nil? or oldtbl.create_syntax.nil? then
              newtbl.save
              say "[new]: server:#{host} database:#{newtbl.database_name} table:#{newtbl.table_name}"
            elsif newtbl.created_at != oldtbl.created_at then
              newtbl.save
              say "[changed]: server:#{host} database:#{newtbl.database_name} table:#{newtbl.table_name}"
            end
        end
      end # TTT::TABLE.all

      # Dropped table detection
      TTT::TableDefinition.find_most_recent_versions(host).each do |tbl|
        g=TTT::TABLE.find_by_TABLE_SCHEMA_and_TABLE_NAME(tbl.database_name, tbl.table_name)
        if g.nil? then
          TTT::TableDefinition.record_timestamps = false
          TTT::TableDefinition.new(
            :server => host,
            :database_name => tbl.database_name,
            :table_name  => tbl.table_name,
            :create_syntax => nil,
            :run_time => runtime,
            :created_at => tbl.created_at,
            :updated_at => "0000-00-00 00:00:00"
          ).save
          TTT::TableDefinition.record_timestamps = true
          say "[deleted]: server:#{host} database:#{tbl.database_name} table:#{tbl.table_name}"
        end
      end
    rescue Mysql::Error => mye
      if mye.errno == MYSQL_CONNECT_ERROR
        say "[unreachable]: server:#{host}"
        TTT::TableDefinition.record_timestamps = false
        TTT::TableDefinition.new(
          :server => host,
          :database_name => nil,
          :table_name  => nil,
          :create_syntax => nil,
          :run_time => Runtime,
          :created_at => "0000-00-00 00:00:00",
          :updated_at => "0000-00-00 00:00:00"
        ).save
        TTT::TableDefinition.record_timestamps = true
      else
        raise mye
      end
    end
  end
end
