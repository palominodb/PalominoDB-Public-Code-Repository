require 'ttt'
require 'ttt/collector'
require 'ttt/table'

class TablesController < ApplicationController
  def show
    @table=Table.find(params[:server_id], params[:database_id], params[:id])
    #update_graph
  end
end
