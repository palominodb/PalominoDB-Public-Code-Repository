require 'ttt'
require 'ttt/db'

class GraphsController < ApplicationController
  def index
    @servers=TTT::Server.all
    @databases=TTT::Database.all
    r=Rrdtool.new
    r.database_graph(@databases, @since_string, :full)
    r.server_graph(@servers, @since_string, :full)
  end

  def show

  end
end

