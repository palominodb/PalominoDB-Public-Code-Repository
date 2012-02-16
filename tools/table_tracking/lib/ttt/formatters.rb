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
