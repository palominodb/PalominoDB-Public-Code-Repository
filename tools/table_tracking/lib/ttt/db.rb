require 'rubygems'
require 'activerecord'
require 'ttt/information_schema'
require 'ttt/table_definition'
require 'ttt/table_volume'

module TTT
  # This class wraps some implementation details about accessing
  # the various databases TTT connects to.
  # In general all you should need to do is pass a Hash of
  # options loaded from a config.yml.
  class Db
    # Establishes a connection to the TTT database.
    # opts can either be ActiveRecord options, or TTT options.
    # if it contains the key 'ttt_connection', then it's assumed to be
    # TTT options.
    # if it contains 'adapter' or :adapter, then it's assumed to be
    # ActiveRecord options.
    # Either way, it winds up being options to: ActiveRecord::Base#establish_conneciton so you should see that document for details.
    def self.open(opts)
      if opts.has_key? "ttt_connection" then
        ActiveRecord::Base.establish_connection(opts["ttt_connection"])
      elsif opts.has_key? "adapter" or opts.has_key? :adapter then
        ActiveRecord::Base.establish_connection(opts)
      else
        raise ArgumentError.new("Bad connection information")
      end
    end

    # Runs TTT specific migrations. At the moment, all this does
    # is create the standard tables.
    # If a file containing an ActiveRecord::Migration subclass is found
    # under <gems dir>/table-tracking-toolkit-<version>/lib/ttt/db/
    # Then it will be run. No questions asked.
    def self.migrate
      ActiveRecord::Migrator.migrate( File.dirname(__FILE__) + "/db", nil )
    end
  end
end
