require 'rubygems'
require 'activerecord'
require 'ttt/information_schema'
require 'ttt/table_definition'
require 'ttt/table_volume'

module TTT
  #class SchemaMigration < ActiveRecord::Base
  #end
  #class Migration < ActiveRecord::Migration ; end
  #class Migration < ActiveRecord::Migration
  #  @@migs=[]
  #  def self.register(class_name, order)
  #    @@migs[order] = [] if !@@migs[order]
  #    @@migs[order] << class_name
  #  end
  #  def self.do_migrate(dir=:up)
  #    if !SchemaMigration.table_exists?
  #      ActiveRecord::Migration.create_table :schema_migrations do |tbl|
  #        tbl.string :mig_name
  #      end
  #    end
  #    # XXX: Figure out why we get called again with an array.
  #    # XXX: I suspect it has to do with some "polymorphism" in ruby.
  #    unless super.class == Array
  #      super.migrate dir unless SchemaMigration.exists?(:mig_name => self.name)
  #      SchemaMigration.new(:mig_name => self.name).save
  #    else
  #      super.each do |sup|
  #        puts "suupuuppupppppp: " + sup.name
  #      end
  #    end
  #  end
  #  def self.each
  #    @@migs.each do |sym_ary|
  #      sym_ary.each do |sym|
  #        yield(TTT.const_get(sym))
  #      end
  #    end
  #  end
  #  def self.migs
  #    @@migs
  #  end
  #end
  class Db
    def self.open(opts)
      if opts.has_key? "ttt_connection" then
        ActiveRecord::Base.establish_connection(opts["ttt_connection"])
      elsif opts.has_key? "adapter" then
        ActiveRecord::Base.establish_connection(opts)
      else
        raise ArgumentError.new("Bad connection information")
      end
    end
    def self.migrate
      ActiveRecord::Migrator.migrate( File.dirname(__FILE__) + "/db", nil )
      #ActiveRecord::Migration.verbose = false
      #Dir.glob( File.dirname(__FILE__) + "/db/*" ).each do |mig|
      #  Kernel.load mig
      #end
      #Migration.each do |mig|
      #  mig.migrate
      #end
    end
  end
end
