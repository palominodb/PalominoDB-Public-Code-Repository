class ServersController < ApplicationController
  def index
    @servers=Server.all
    #pp @servers
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

end
