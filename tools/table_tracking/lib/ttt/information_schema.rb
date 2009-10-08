require 'rubygems'
require 'activerecord'

module TTT
  class InformationSchema < ActiveRecord::Base
    self.abstract_class = true
    def self.connect(host, cfg)
      establish_connection( {
        "adapter" => "mysql",
        "host" => host,
        "database" => "information_schema",
        }.merge(cfg["dsn_connection"])
      )
    end
  end
  class TABLE < InformationSchema
    set_table_name :TABLES
    def create_syntax
      schema=read_attribute(:TABLE_SCHEMA)
      name=read_attribute(:TABLE_NAME)
      connection.execute("SHOW CREATE TABLE #{schema}.#{name}").fetch_hash()["Create Table"]
    end
  end
  class SCHEMA < InformationSchema
    set_table_name :SCHEMATA
  end

end

