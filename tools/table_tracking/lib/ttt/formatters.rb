# formatters.rb
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
require 'ttt/collector'

module TTT
  class Formatter
    attr :stream
    attr :cfg
    @@loaded_formatters = false
    @@formatters = {}
    @@runners = {}

    class_inheritable_accessor :media

    def initialize(stream, cfg)
      @stream=stream
      @cfg=cfg
    end

    def format(rows, *args)
      raise Exception, "Use a real formatter."
    end

    def self.humanize(name)
      ActiveRecord::Base.human_attribute_name(name)
    end

    def reject_ignores(rows)
      if @cfg.key? "report_ignore"
        return rows.reject do |r|
          server_schema_table=[r.server, r.database_name, r.table_name].join(".")
          do_rej=false
          unless @cfg["report_ignore"][r.collector.to_s].nil?
            @cfg["report_ignore"][r.collector.to_s].each do |reg|
              do_rej = !Regexp.new(reg).match(server_schema_table).nil?
              break if do_rej
            end
          end
          if !do_rej
            unless @cfg['report_ignore']['global'].nil?
              @cfg["report_ignore"]["global"].each do |reg|
                do_rej = !Regexp.new(reg).match(server_schema_table).nil?
                break if do_rej
              end
            end
          end
          do_rej
        end
      end
      return rows
    end

    def self.for(collector,output_media)
      @@formatters[collector] = {} if @@formatters[collector].nil?
      @@formatters[collector][output_media] = Proc.new
    end
    def self.runner_for(media)
      @@runners[media] = self
      self.media=media
    end
    def self.get_runner_for(media)
      self.load_all
      @@runners[media]
    end
    def self.get_formatter_for(collector, mtype=self.media)
      CollectorRegistry.load
      @@formatters[collector][mtype]
    end

    def need_option(key)
      unless @cfg["formatter_options"].key? media.to_s and @cfg["formatter_options"][media.to_s].key? key
        raise NameError, "Missing formatter_options.#{media.to_s}.#{key} in config."
      end
      @cfg["formatter_options"][media.to_s][key]
    end

    def want_option(key, value=nil)
      if @cfg["formatter_options"][media.to_s].key? key
        @cfg["formatter_options"][media.to_s][key]
      else
        value
      end
    end

    # Loads all formatters under: <gems path>/table-tracking-toolkit-<version>/lib/ttt/format/*
    # This must be called before formatters will function.
    def self.load_all
      unless @@loaded_formatters
        Dir.glob( File.dirname(__FILE__) + "/format/*" ).each do |col|
          Kernel.load col
        end
        @@loaded_formatters=true
      end
    end

  end
end
