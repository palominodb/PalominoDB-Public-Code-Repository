# Copyright (c) 2009-2010, PalominoDB, Inc.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
# 
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
# 
#   * Neither the name of PalominoDB, Inc. nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
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
