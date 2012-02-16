require 'ttt'
require 'ttt/collector'
require 'ttt/table'

class DatabasesController < ApplicationController
  def index
    @databases=TTT::Schema.all(:order => 'cached_size DESC')
    
  end
  def show
    @database=Database.find(params[:server_id], params[:id])
    if @database.nil?
      render :file => 'shared/404', :status => 404
    else
      r=Rrdtool.new
      r.database_graph(@database, @since_string, :full)
    end
  end
end
