require 'rubygems'
require 'activerecord'
require 'ttt/db'
require 'ttt/table'
require 'ttt/collector'
require 'ttt/table_volume'
require 'fileutils'

module TTT
  class RRDFormatter < Formatter
    runner_for :rrd
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
      #elsif updint.to_i > 2.days
      #  raise ArgumentError, "option update_interval must be less than 2 days."
      end
      updint=updint.to_i
      last_run=TTT::Collector.get_last_run(:volume)
      TTT::TableVolume.servers.each do |srv|
        rrd_path="#{path}/#{srv}/server_#{srv}.rrd"
        #sz=TTT::TableVolume.server_sizes(srv)
        last=lastupd_rrd(rrd_path)
        create_rrd(rrd_path, updint, TTT::TableVolume.first(:conditions => ['server = ?', srv], :order => :id).run_time)
        TTT::TableVolume.runs(last).each do |run|
          s=TTT::TableVolume.server_sizes(srv, run)
          next if s.unreachable?
          update_rrd(rrd_path, run, [s.data_length, s.index_length, 'U'])
        end
        TTT::TableVolume.schemas(srv).each do |sch|
          next if sch.unreachable?
          rrd_path="#{path}/#{srv}/database_#{sch.database_name}.rrd"
          last=lastupd_rrd(rrd_path)
          rt=TTT::TableVolume.first(:conditions => ['server = ? and database_name = ?', srv, sch.database_name], :order => :id)
          unless rt.nil?
            create_rrd(rrd_path, updint, rt.run_time)
          end
          TTT::TableVolume.runs(last).each do |run|
            d=TTT::TableVolume.database_sizes(srv, sch.database_name, run)
            next if d.deleted?
            update_rrd(rrd_path, run, [d.data_length, d.index_length, 'U'])
          end
        end
        TTT::TableVolume.tables(srv).each do |tbl|
          next if tbl.unreachable?
          rrd_path="#{path}/#{srv}/#{tbl.database_name}/#{tbl.table_name}.rrd"
          last=lastupd_rrd(rrd_path)
          create_rrd(rrd_path, updint, TTT::TableVolume.first(:conditions => ['server = ?', srv], :order => :id).run_time)
          TTT::TableVolume.find(:all, :conditions => ['server = ? and database_name = ? and table_name = ? and run_time > ?', srv, tbl.database_name, tbl.table_name, last]).each do |r|
            update_rrd(rrd_path, r.run_time, [r.data_length, r.index_length, r.data_free])
          end
        end
      end
      #reject_ignores(TTT::TrackingTable.tables[:volume].find_most_recent_versions).each do |r|
      #  updates=[
      #    ["#{path}/#{r.server}/server_#{r.server}.rrd", Proc.new do || TTT::TableVolume.sum('data_length', :conditions => ['server = ? and run_time = ?', r.
      #  ]
        #rra_path="#{path}/#{r.server}/#{r.database_name}/#{r.table_name}.rrd"
        #next if r.unreachable?
        #if File.exist? rra_path
        #  lastupd=%x{rrdtool last #{rra_path}}.chomp
        #  lastupd=Time.at lastupd.to_i
        #  TTT::TrackingTable.tables[:volume].find(:all, :conditions => ['run_time > ? AND server = ? AND database_name = ? AND table_name = ?', lastupd, r.server, r.database_name, r.table_name ]).each do |nr|
        #    cmd="rrdtool update #{rra_path} #{nr.run_time.to_i}:#{nr.data_length}:#{nr.index_length}:#{nr.data_free}"
        #    puts cmd
        #    %x{#{cmd}}
        #  end
        #else
        #  TTT::TrackingTable.tables[:volume].find(:all, :conditions => ['server = ? AND database_name = ? AND table_name = ?',
        #                                            r.server, r.database_name, r.table_name ]).each do |nr|

        #                                            end
        #end
      #end
      #true
    end

    def lastupd_rrd(path)
      #lastupd=nil
      begin
        lastupd=Time.at %x{rrdtool last #{path}}.chomp.to_i
      rescue Exception
        nil
      end
      #lastupd
    end

    def update_rrd(path, time_at, values)
      cmd="rrdtool update #{path} #{time_at.class == Time ? time_at.to_i : time_at}:#{values.join(":")}"
      puts cmd
      %x{#{cmd}}
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
        cmd=["rrdtool create #{path}",
          "--step #{step}", "--start #{(start.class == Time ? start.to_i : start)-10}",
          "DS:data_length:GAUGE:#{step*2}:U:U",
          "DS:index_length:GAUGE:#{step*2}:U:U",
          "DS:data_free:GAUGE:#{step*2}:U:U",
        rra_s
        ].flatten.join(' ')
        puts cmd
        %x{#{cmd}}
      end
    end
  end
end
