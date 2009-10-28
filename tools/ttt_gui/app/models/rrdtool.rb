require 'table'
require 'helpers/application_helper'
class RrdError < Exception ; end
class Rrdtool
  include ApplicationHelper
  attr_reader :path
  def initialize(path=nil)
    @path=path || TTT_CONFIG['formatter_options']['rrd']['bin']
    raise ArgumentError, "Argument 'path' must not be nil" if @path.nil?
  end
  def exec(params)
    pp params
    res=%x{#{path} #{params} }
    pp res
    if res =~ /^ERROR:(.*)/
      raise RrdError, $1
    end
    res
  end

  def server_graph(servers,since, type=:full)
    msgs=[]
    ok=true
    [servers].flatten.each do |srv|
      rrd_path=TTT_CONFIG['formatter_options']['rrd']['path']+"/#{srv.name}/server_#{srv.name}.rrd"
      opts = common_opts("server_#{srv.name}", since, type, "Server Aggregate - #{srv.name}")

      opts << [
        ["data_length", ["AREA%s:STACK", "#00ff40"]],
        ["index_length", ["AREA%s", "#0040ff"]]
        #["data_free", ["LINE2%s", "#0f00f0"]],
      ].collect do |ds|
        common_ds_opts(ds, rrd_path)
      end

      opts.flatten!

      begin
        exec(opts.join(" "))
      rescue RrdError => e
        msgs << e.message
        ok=false
      end
    end
    [ok,msgs]
  end

  def database_graph(databases,since, type=:full)
    msgs=[]
    ok=true
    [databases].flatten.each do |db|
      rrd_path=TTT_CONFIG['formatter_options']['rrd']['path']+"/#{db.server}/database_#{db.name}.rrd"
      opts = common_opts("database_#{db.server}_#{db.name}", since, type, "Database Aggregate - #{db.server}.#{db.name}")

      opts << [
        ["data_length", ["AREA%s:STACK", "#00ff40"]],
        ["index_length", ["AREA%s", "#0040ff"]]
        #["data_free", ["LINE2%s", "#0f00f0"]],
      ].collect do |ds|
        common_ds_opts(ds, rrd_path)
      end

      opts.flatten!

      begin
        exec(opts.join(" "))
      rescue RrdError => e
        msgs << e.message
        ok=false
      end
    end
    [ok,msgs]
  end

  def table_graph(tables,since, type=:full)
    msgs=[]
    ok=true
    [tables].flatten.each do |tbl|
      rrd_path=TTT_CONFIG['formatter_options']['rrd']['path']+"/#{tbl.server}/#{tbl.database}/#{tbl.name}.rrd"
      opts = common_opts("table_#{tbl.server}_#{tbl.database}_#{tbl.name}", since, type, "Table - #{tbl.server}.#{tbl.database}.#{tbl.name}")

      opts << [
        ["data_length", ["AREA%s:STACK", "#00ff40"]],
        ["index_length", ["AREA%s", "#0040ff"]],
        ["data_free", ["LINE2%s", "#0f00f0"]],
      ].collect do |ds|
        common_ds_opts(ds, rrd_path)
      end

      opts.flatten!

      begin
        exec(opts.join(" "))
      rescue RrdError => e
        msgs << e.message
        ok=false
      end
    end
    [ok,msgs]
  end

  private
  def common_opts(path_frag,since,type, title)
    o=[
      "graph", "#{RAILS_ROOT}/public/images/graphs/" + path_frag + ".#{since}.#{type.to_s}.png",
      "-s", (str_to_time since).to_i, "--width", type == :full ? 640 : 128,
      "-e", "now", "--title", %Q{"#{title}"}
    ]
    if type == :thumb
      o<< "-j" << "--height" << 16
    end
    o
  end

  def common_ds_opts(ds, rrd_path)
    dsname=ds[0]
    gitems=ds[1..-1]
    ret=[]
    ret<<"DEF:avg_#{dsname}=#{rrd_path}:#{dsname}:AVERAGE"
    ret<<"DEF:min_#{dsname}=#{rrd_path}:#{dsname}:MIN"
    ret<<"DEF:max_#{dsname}=#{rrd_path}:#{dsname}:MAX"
    ret<<"VDEF:v_last_#{dsname}=avg_#{dsname},LAST"
    ret<<"VDEF:v_avg_#{dsname}=avg_#{dsname},AVERAGE"
    ret<<"VDEF:v_min_#{dsname}=avg_#{dsname},MINIMUM"
    ret<<"VDEF:v_max_#{dsname}=avg_#{dsname},MAXIMUM"
    gitems.each do |gi|
      ret<< gi[0] % %Q{:avg_#{dsname}#{gi[1]}:"#{dsname.titleize}"}
    end
    ret<< %Q{GPRINT:v_last_#{dsname}:"Current\\: %0.2lf%s"}
    ret<< %Q{GPRINT:v_avg_#{dsname}:"Avg\\: %0.2lf%s"}
    ret<< %Q{GPRINT:v_min_#{dsname}:"Min\\: %0.2lf%s"}
    ret<< %Q{GPRINT:v_max_#{dsname}:"Max\\: %0.2lf%s"}
    ret<< %Q{COMMENT:"\\s"}
    ret<< %Q{COMMENT:"\\s"}
    ret
  end
end
