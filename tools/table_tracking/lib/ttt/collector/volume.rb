require 'rubygems'
require 'ttt/db'
require 'ttt/table_volume'

module TTT
  class VolumeCollector < Collector
    register "volume"
    def self.collect(host,cfg)
      TTT::InformationSchema.connect(host,cfg)
      begin
        TTT::TABLE.all.each do |tbl|
          next if tbl.TABLE_SCHEMA == "information_schema"
          TTT::TableVolume.new(
            :server => host,
            :database_name => tbl.TABLE_SCHEMA,
            :table_name => tbl.TABLE_NAME,
            :run_time => Runtime,
            :bytes => tbl.DATA_LENGTH
          ).save
          say "[volume] server:#{host} schema:#{tbl.TABLE_SCHEMA} table:#{tbl.TABLE_NAME} bytes:#{tbl.DATA_LENGTH}" if(verbose)
        end # Table.all

      rescue Mysql::Error => mye
        if mye.errno == MYSQL_CONNECT_ERROR
          say "[unreachable]: server:#{host}"
          TTT::TableVolume.record_timestamps = false
          TTT::TableVolume.new(
            :server => host,
            :database_name => nil,
            :table_name => nil,
            :bytes => nil,
            :run_time => Runtime,
            :created_at => "0000-00-00 00:00:00",
            :updated_at => "0000-00-00 00:00:00"
          ).save
          TTT::TableVolume.record_timestamps = true
        else
          raise mye
        end
      end
    end
  end
end
