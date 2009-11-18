require 'rubygems'
require 'activerecord'

module TTT
  # Base class for connections to collection hosts.
  # Please see ActiveRecord for how these classes work.
  class InformationSchema < ActiveRecord::Base
    self.abstract_class = true
    @@connected_host=nil
    def self.connect(host, cfg)
      @@connected_host=host
      establish_connection( {
        "adapter" => "mysql",
        "host" => host,
        "database" => "information_schema",
        }.merge(cfg["dsn_connection"])
      )
    end
    def self.get_connected_host
      @@connected_host
    end
    private
    def establish_connection(*args)
      super(args)
    end
  end

  # Access to the "TABLES" table from information_schema database
  class TABLE < InformationSchema
    set_table_name :TABLES
    
    # Returns the table's data definition.
    # If the table is a regular table, then a statement
    # such as "CREATE TABLE.." will be returned.
    # If the table is a view, then a statement such as "CREATE VIEW.."
    # will be returned.
    def create_syntax
      schema=read_attribute(:TABLE_SCHEMA)
      name=read_attribute(:TABLE_NAME)
      syn=connection.execute("SHOW CREATE TABLE `#{schema}`.`#{name}`").fetch_hash()
      if read_attribute(:TABLE_TYPE) == "VIEW"
        syn["Create View"]
      else
        syn["Create Table"]
      end
    end
    # Returns true, if the table is determined to be a "system table".
    # That is, a table generated dynamically by MySQL.
    def system_table?
      read_attribute(:TABLE_TYPE) == "SYSTEM VIEW" or (read_attribute(:CREATE_TIME).nil? and read_attribute(:UPDATE_TIME).nil?)
    end
  end

  # Access to the "SCHEMATA" table from 'information_schema' database
  class SCHEMA < InformationSchema
    set_table_name :SCHEMATA
  end

end

