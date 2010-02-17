require 'rubygems'
require 'active_record'
require 'ttt/db'
require 'ttt/table'
require 'ttt/collector'
require 'ttt/table_volume'
require 'fileutils'

module TTT
  class RRDFormatter < Formatter
    runner_for :rrd

    class RrdError < RuntimeError ; end

    def initialize(stream, cfg)
      super(stream, cfg)

      unless @path=need_option('bin')
        raise ArgumentError, "bin: #{need_option('bin')} apparently invalid?"
      end
    end

    class RRDTool
      def initialize(path)
        @rrd_io = IO.popen(path + " -", 'r+')
        @path = path
        unless @rrd_io
          raise ArgumentError, "bin: #{path} apparently invalid?"
        end
      end
      def exec_rrd(*args)
        puts "executing: " + args.join(' ')
        @rrd_io.puts(args.join(' '))
        o=@rrd_io.readpartial(4096).split("\n")
        o.map! { |l| l.chomp }
        unless o[-1] =~ /^OK u:\d+.\d+ s:\d+.\d+ r:\d+.\d+$/
          raise RrdError, "RRD Returned: #{o} intead of OK"
        end
        o.pop
        o
      end

      def lastupd_rrd(path)
        begin
          lastupd=Time.at(exec_rrd('last', path)[0].chomp.to_i)
        rescue Exception
          Time.at 0
        end
      end

      def update_rrd(path, time_at, values)
        if time_at.class == Time
          time_at = time_at.to_i
        end
        exec_rrd('update', path, [time_at, values].flatten.join(':'))
      end

      def create_rrd(path, step, start)
        if !File.exist? path
          FileUtils.makedirs(File.dirname(path))
          rra_s=[]
          ["AVERAGE", "MAX", "MIN"].each do |rra|
            [[1, 48.hours], [2, 2.week], [4, 1.month], [8, 6.months], [16, 1.year], [32, 2.year]].each do |cyl|
              incr=cyl[0]
              wind=cyl[1]
              rra_s << %Q{RRA:#{rra}:0.25:#{incr}:#{wind/step}} if step < wind
            end
          end
          cmd=['create', path,
            '--step', step, "--start #{(start.class == Time ? start.to_i : start)-1}",
            "DS:data_length:GAUGE:#{step*2}:U:U",
          "DS:index_length:GAUGE:#{step*2}:U:U",
          "DS:data_free:GAUGE:#{step*2}:U:U",
          rra_s
          ].flatten
          exec_rrd(cmd)
        end
      end
    end

    def format(rows, *args)
      options=args.extract_options!
      path=need_option("path")
      updint=need_option("update_interval")
      if updint =~ /(\d+(?:\.?\d+)?)([mhd])?/
        case $2
        when 'm'
          updint=$1.to_f.minutes
        when 'h'
          updint=$1.to_f.hours
        when 'd'
          updint=$1.to_f.days
        end
      elsif updint.to_i == 0
        raise ArgumentError, "option update_interval must resolve to a number greater than 0."
      end
      updint=updint.to_i
      last_run=TTT::CollectorRun.find_by_collector(:volume).last_run
      runs=TTT::TableVolume.runs

      rrdtool=RRDTool.new(@path)
      TTT::TableVolume.servers.each do |srv|
        rrd_path="#{path}/#{srv}/server_#{srv}.rrd"
        last=rrdtool.lastupd_rrd(rrd_path)
        unless File.exists? rrd_path
          rrdtool.create_rrd(rrd_path, updint, TTT::TableVolume.first(:conditions => ['server = ?', srv], :order => :id).run_time)
        end
        if last < last_run
          max_run = TTT::Server.find_by_name(srv).updated_at
          runs.select { |r| r > last }.each do |run|
            next if run > max_run
            begin
              s=TTT::TableVolume.server_sizes0(srv, run)
            rescue Exception # no rows found
              next
            end
            rrdtool.update_rrd(rrd_path, run, [s.data_length, s.index_length, 'U'])
          end
        end
      end

      TTT::TableVolume.schemas.each do |sch|
        next if sch.name == 'information_schema'
        srv = sch.server.name
        rrd_path="#{path}/#{srv}/database_#{sch.name}.rrd"
        last=rrdtool.lastupd_rrd(rrd_path)
        unless File.exists? rrd_path
          rrdtool.create_rrd(rrd_path, updint, TTT::TableVolume.first(:conditions => ['server = ? AND database_name = ?', srv, sch.name], :order => :id).run_time)
        end
        if last < last_run
          max_run = sch.updated_at
          runs.select { |r| r > last }.each do |run|
            next if run > max_run
            begin
              s=TTT::TableVolume.database_sizes0(srv, sch.name, run)
            rescue Exception # no rows found
              next
            end
            rrdtool.update_rrd(rrd_path, run, [s.data_length, s.index_length, 'U'])
          end
        end
      end

      TTT::TableVolume.tables.each do |tbl|
        srv = tbl.schema.server.name
        sch = tbl.schema.name
        next if sch == 'information_schema'
        rrd_path="#{path}/#{srv}/#{sch}/#{tbl.name}.rrd"

        last=rrdtool.lastupd_rrd(rrd_path)
        unless File.exists? rrd_path
          begin
            rrdtool.create_rrd(rrd_path, updint, TTT::TableVolume.first(:conditions => ['server = ? AND database_name = ? AND table_name = ?', srv, sch, tbl.name], :order => :id).run_time)
          rescue NoMethodError => e
            puts "WARNING: tables/history mis-match on: #{srv}.#{sch}.#{tbl.name}"
          end
        end
        if last < last_run
          max_run = tbl.updated_at
          runs.select { |r| r > last }.each do |run|
            next if run > max_run
            s=TTT::TableVolume.first(:conditions => ['run_time = ? AND server = ? AND database_name = ? AND table_name = ?', run, srv, sch, tbl.name], :order => :id)
            unless s.nil?
              rrdtool.update_rrd(rrd_path, run, [s.data_length, s.index_length, s.data_free])
            end
          end # runs
        end # last < last_run

      end

      0
    end

  end # RRDFormatter
end # module TTT
