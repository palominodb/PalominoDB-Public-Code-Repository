require 'ttt/collector'
require 'ttt/formatters'
class DashboardController < ApplicationController
  def index
    if params[:id] and TTT::TrackingTable.tables.key? params[:id]
      @stats=[params[:id]]
    else
      @stats=(TTT::TrackingTable.tables.each_key.map {|k| k.to_s }).sort
    end
    @changes={}
    #@stats.each do |s|
    #  @changes[s] = {}
    #  #TTT::TrackingTable.tables[s.to_sym]
    #  #TTT::TrackingTable.tables[s.to_sym].servers.each do |srv|
    #  #  @changes[s][srv] = 0 if !@changes.key? srv
    #  #  TTT::TrackingTable.tables[s.to_sym].find_most_recent_versions({:conditions => ['server = ?', srv]}, @since_string ? str_to_time(@since_string) : TTT::Collector.get_last_run(s) ).each do |v|
    #  #    @changes[s][srv] += 1 if(v.tchanged?)
    #  #  end
    #  #end
    #end
  end

  def overview
    index
    render :index
  end

end
