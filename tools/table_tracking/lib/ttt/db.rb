# db.rb
# Copyright (C) 2009-2013 PalominoDB, Inc.
# 
# You may contact the maintainers at eng@palominodb.com.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

require 'rubygems'
require 'active_record'
require 'sqlite3' # For catching exceptions
require 'ttt/table'
require 'ttt/information_schema'
require 'ttt/server'

module TTT
  # This class wraps some implementation details about accessing
  # the various databases TTT connects to.
  # In general all you should need to do is pass a Hash of
  # options loaded from a config.yml.
  class Db
    @@app_config=nil
    # Establishes a connection to the TTT database.
    # See sample-config.yml for an example of TTT configuration.
    # This method uses the key 'ttt_connection'. See ActiveRecord::Base#establish_conneciton for details on the options that are available.
    def self.open(opts)
      if opts.has_key? "ttt_connection" then
        @@app_config=opts
        ActiveRecord::Base.establish_connection(opts["ttt_connection"])
      else
        raise ArgumentError.new("Bad connection information")
      end
    end

    # Generates a new interface class.
    # Do not store this result in a constant
    # as that will trigger some ActiveRecord magic
    # and reset the connection that was made.
    #
    # Note: This method is only for accessing 'DSN' tables.
    # TTT tables should have dedicated classes.
    def self.open_schema(host, schema, table)
      nc=Class.new(ActiveRecord::Base)
      c=@@app_config['dsn_connection'].merge(
          {
            'adapter'  => 'mysql',
            'host'     => host,
            'database' => schema
          }
      )
      nc.establish_connection(c)
      nc.set_table_name table
      nc
    end

    # Runs TTT specific migrations. At the moment, all this does
    # is create the standard tables.
    # If a file containing an ActiveRecord::Migration subclass is found
    # under <gems dir>/table-tracking-toolkit-<version>/lib/ttt/db/
    # Then it will be run. No questions asked.
    # Every file in that directory must be of the form:
    # <number>_<description_with_underscores>.rb
    # ActiveRecord will *actually* throw an exception otherwise.
    def self.migrate
      m=ActiveRecord::Migrator.new(:up, File.dirname(__FILE__) + "/db" )
      unless m.pending_migrations.empty?
        m.migrate
      end
    end

  end
end
