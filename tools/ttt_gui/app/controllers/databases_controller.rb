require 'ttt'
require 'ttt/collector'
require 'ttt/table'

class DatabasesController < ApplicationController
  def index
    @databases=Database.all
    
  end
  def show
    @database=Database.find(params[:server_id], params[:id])
    if @database.nil?
      render :file => 'shared/404', :status => 404
    else
    update_graph(params[:server_id], @database.name, "Database Aggregate - #{@database.server}.#{@database.name}")
    end
  end
  private
  def update_graph(server_name, database_name, title, type=:full)
    rrd_path=TTT_CONFIG['formatter_options']['rrd']['path']+"/#{server_name}/database_#{database_name}.rrd"
    rrd_bin=TTT_CONFIG['formatter_options']['rrd']['bin']
    rrd_defs=[]
    rrd_vdefs=[]
    opts = [
      "graph",
      "#{RAILS_ROOT}/public/images/graphs/database_#{server_name}_#{database_name}_#{@since_string}.png",
      "-s", "#{(str_to_time @since_string).to_i}", "--width", type == :full ? 640 : 48,
      "-e", "now", "--title", %Q{"#{title}"}
    ]

    if type == :summary
      opts << "-j"
      opts << "--height" << 16
    end

    opts<<[
      ["data_length", ["AREA%s:STACK", "#00ff40"], ["LINE%s", "#000000"]],
      ["index_length", ["AREA%s", "#0040ff"]]
      #["data_free", ["LINE2%s", "#0f00f0"]]
    ].collect do |ds|
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
      if type == :full
        ret<< %Q{GPRINT:v_last_#{dsname}:"Current\\: %0.2lf%s"}
        ret<< %Q{GPRINT:v_avg_#{dsname}:"Avg\\: %0.2lf%s"}
        ret<< %Q{GPRINT:v_min_#{dsname}:"Min\\: %0.2lf%s"}
        ret<< %Q{GPRINT:v_max_#{dsname}:"Max\\: %0.2lf%s"}
        ret<< %Q{COMMENT:"\\s"}
        ret<< %Q{COMMENT:"\\s"}
      end
      ret
    end

    opts.flatten!
    pp opts

    begin
      r=Rrdtool.new(rrd_bin.nil? ? "/opt/local/bin/rrdtool" : rrd_bin)
      r.exec(opts.join(" "))
    rescue RrdError => e
      logger.error "RRDtool: "+e.message
      flash[:rrd_error] = e.message
    end
  end
end
