require 'ttt'
require 'ttt/server'
require 'ttt/table_volume'

class TopTablesController < ApplicationController
  def show
    s_id=params[:server_id]
    d_id=params[:database_id]
    lim=params[:limit].to_i
    days=params[:days].to_i
    percent=(params[:percent] == "" ? 0.0/0.0 : params[:percent].to_f)

    if lim != 0 and days == 0 and percent.nan?
      flash[:missing_variables] = nil
      @type = :top_N
      if s_id.nil?
        @tables=TTT::Table.all(:limit => lim, :order => 'database_tables.cached_size DESC')
      else
        if d_id.nil?
          @tables=TTT::Server.find_by_name(s_id).tables.all(:limit => lim, :order => 'database_tables.cached_size DESC')
        else
          @tables=TTT::Server.find_by_name(s_id).schemas.find_by_name(d_id).tables.all(:limit => lim, :order => 'database_tables.cached_size DESC')
        end
      end
    elsif days != 0 and !percent.nan?
      flash[:missing_variables] = nil
      @type = :top_Pct
      @raw_tables={}
      #select *,min(id),max(id) from table_volumes where run_time>'2009-10-25 00:00:00' group by server,database_name,table_name;
      min_maxes = TTT::TableVolume.all(:select => 'min(id) as min_id, max(id) as max_id', :conditions => [(s_id.nil? ? %Q{} : %Q{server = '#{s_id}' and } ) + %Q{database_name not null and table_name not null and run_time > ?}, days.days.ago], :group => 'server,database_name,table_name')
      min_maxes.each do |t|
        logger.debug [t.min_id, t.max_id]
        min_tbl,max_tbl=TTT::TableVolume.find(t.min_id, t.max_id)
        next if min_tbl.nil? or max_tbl.nil? or min_tbl.deleted? or max_tbl.deleted?
        @raw_tables[[min_tbl.server,min_tbl.database_name,min_tbl.table_name]] = (max_tbl.size+0.0-min_tbl.size+0.0)/min_tbl.size+0.0 # Force into Float
      end
      @tables=(((@raw_tables.to_a).select { |t| logger.debug("tbl (#{t[0].join('.')}) pct grow: #{t[1]}") ; t[1] > percent }).sort { |x,y| y[1]<=>x[1] })[0,lim]
    elsif lim == 0 and  days == 0 and percent == 0 
      flash[:missing_variables] = "Please enter 'N', or 'N', 'Pct', and 'Days'." if params[:commit]
      @tables=[]
    end
  end
end
