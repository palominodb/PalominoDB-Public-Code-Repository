require 'server'
class ServersController < ApplicationController
  rescue_from ServerNotFound, :with => :server_not_found
  def index
    @servers=Server.all
    @server_sizes={}
    @servers.each do |s|
      @server_sizes[s] = Server.get_size(s)
    end
  end

  def show
    @server=Server.find(params[:id])
    r=Rrdtool.new
    r.server_graph(@server, @since_string, :full)
  end

  private
  def server_not_found
    flash[:error] = "You have been redirected back here because you tried to navigate to an unknown server."
    redirect_to :servers
  end
end
