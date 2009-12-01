require 'rubygems'
require 'active_record'
require 'ttt/db'

module TTT
  class Server < ActiveRecord::Base
    has_many :schemas, :class_name => "TTT::Schema"
    has_many :tables, :through => :schemas, :class_name => "TTT::Table"
  end

  class Schema < ActiveRecord::Base
    set_table_name :server_schemas
    belongs_to :server, :class_name => "TTT::Server"
    has_many :tables, :class_name => "TTT::Table"
  end

  class Table < ActiveRecord::Base
    set_table_name :database_tables
    belongs_to :schema, :class_name => "TTT::Schema"

    def size
      cached_size
    end
  end
end
