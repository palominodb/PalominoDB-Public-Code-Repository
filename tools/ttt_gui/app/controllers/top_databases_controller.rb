require 'ttt'
require 'ttt/server'
require 'ttt/table_volume'
class TopDatabasesController < ApplicationController
  def show
    lim=( (params[:limit].nil? or params[:limit].empty?) ? nil : params[:limit].to_i )
    days=( (params[:days].nil? or params[:days].empty?) ? nil : params[:days].to_i )
    percent=( (params[:percent].nil? or params[:percent].empty?) ? 0.0/0.0 : params[:percent].to_f )
    gbytes=( (params[:gbytes].nil? or params[:gbytes].empty?) ? 0.0/0.0 : params[:gbytes].to_f )
    @raw_tables={}
    if !lim.nil? and !days.nil? and !percent.nan?
      flash[:error_message] = nil
      @type = :top_Pct
      #select *,min(id),max(id) from table_volumes where run_time>'2009-10-25 00:00:00' group by server,database_name,table_name;
      min_maxes = TTT::TableVolume.all(:select => 'min(id) as min_id, max(id) as max_id', :conditions => [%Q{database_name not null and table_name not null and run_time > ?}, days.days.ago], :group => 'server,database_name,table_name')
      min_maxes.each do |t|
        logger.debug [t.min_id, t.max_id]
        min_tbl,max_tbl=TTT::TableVolume.find(t.min_id, t.max_id)
        next if min_tbl.nil? or max_tbl.nil? or min_tbl.deleted? or max_tbl.deleted?
        @raw_tables[[min_tbl.server,min_tbl.database_name]] = 0.0 if !@raw_tables.key? [min_tbl.server,min_tbl.database_name]
        @raw_tables[[min_tbl.server,min_tbl.database_name]] += (max_tbl.size+0.0-min_tbl.size+0.0)/min_tbl.size+0.0 # Force into Float
      end
      # turn our hash into an array, select only those with percentages greater than what we want, sort based on percentage, and take the top lim
      @databases=(((@raw_tables.to_a).select { |t| t[1] > percent }).sort { |x,y| y[1]<=>x[1] })[0,lim]
    elsif !lim.nil? and !days.nil? and !gbytes.nan?
      flash[:error_message] = nil
      @type = :top_GB
      #select *,min(id),max(id) from table_volumes where run_time>'2009-10-25 00:00:00' group by server,database_name,table_name;
      min_maxes = TTT::TableVolume.all(:select => 'min(id) as min_id, max(id) as max_id', :conditions => [%Q{database_name not null and table_name not null and run_time > ?}, days.days.ago], :group => 'server,database_name,table_name')
      min_maxes.each do |t|
        logger.debug [t.min_id, t.max_id]
        min_tbl,max_tbl=TTT::TableVolume.find(t.min_id, t.max_id)
        next if min_tbl.nil? or max_tbl.nil? or min_tbl.deleted? or max_tbl.deleted?
        @raw_tables[[min_tbl.server,min_tbl.database_name]] = 0.0 if !@raw_tables.key? [min_tbl.server,min_tbl.database_name]
        @raw_tables[[min_tbl.server,min_tbl.database_name]] += (max_tbl.size+0.0-min_tbl.size+0.0) # Force into Float
      end
      # turn our hash into an array, select only those with percentages greater than what we want, sort based on percentage, and take the top lim
      @databases=(((@raw_tables.to_a).select { |t| t[1] > gbytes.gigabytes}).sort { |x,y| y[1]<=>x[1] })[0,lim]
    else
      flash[:error_message] = "Must specify percentage." if params[:commit]
      @databases=[]
    end

  end
end
