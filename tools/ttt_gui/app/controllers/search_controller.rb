require 'ttt'
require 'ttt/server'
class SearchController < ApplicationController
  def show
    q=params[:q]
    @matched=[]
    TTT::Server.all.each do |srv|
      logger.debug srv.name
      srv.schemas.all.each do |sch|
        sch.tables.all.each do |tbl|
          str="#{srv.name}.#{sch.name}.#{tbl.name}"
          if str =~ /#{params[:q]}/
            @matched << [srv,sch,tbl]
          end
        end
      end
      if @matched[-1].nil? or @matched[-1][0].name != srv
        if srv.name =~ /#{params[:q]}/
          @matched << [srv]
        end
      end
    end
  end
  def server_sel
    unless params[:server].nil?
      redirect_to server_path(params[:server])
    else
      render :file => 'shared/404', :status => 404
    end
  end
end
