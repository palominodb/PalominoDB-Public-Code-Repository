require 'ttt'
require 'ttt/server'
require 'ttt/table_volume'
class TopDatabasesController < ApplicationController
  def show
    lim=params[:limit].to_i
    days=params[:days].to_i
    percent=params[:percent].to_i
    @raw_tables={}
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
    @databases=(((@raw_tables.to_a).select { |t| t[1]*100 > percent }).sort { |x,y| y[1]<=>x[1] })[0,lim]

  end
end
