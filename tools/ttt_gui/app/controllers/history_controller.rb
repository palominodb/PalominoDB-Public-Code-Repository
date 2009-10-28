require 'ttt'
require 'ttt/db'
require 'ttt/table'

class HistoryController < ApplicationController
  def index
  end

  def show
    @table=Table.find(params[:server_id], params[:database_id], params[:id])
    if @table.nil?
       render :file => 'shared/404', :status => 404
    else
      @history=@table.get_history(Time.at(0))
    end
  end

end
