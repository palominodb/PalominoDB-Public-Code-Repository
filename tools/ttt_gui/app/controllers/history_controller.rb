require 'ttt'
require 'ttt/db'
require 'ttt/table'

require 'diff/lcs'
require 'diff/lcs/hunk'

class HistoryController < ApplicationController
  before_filter :show_diffs_session
  def index
    @tables = TTT::TableDefinition.find(:all, :conditions => ['run_time > ?', since_t], :order => 'run_time DESC')
    @views  = TTT::TableView.find(:all, :conditions => ['run_time > ?', since_t], :order => 'run_time DESC')
  end

  def show
    if params[:at]
      @table=Table.find_at(params[:server_id], params[:database_id], params[:id], params[:at])
    else
      @table=Table.find(params[:server_id], params[:database_id], params[:id])
    end
    if @table.nil?
       render :file => 'shared/404', :status => 404
    else
      @history=@table.get_history(Time.at(0))
    end
  end

  private
  def show_diffs_session
    if params[:show_diffs]
      session[:show_diffs] = params[:show_diffs].to_i == 1
    elsif params[:show_diffs].nil? and session[:show_diffs].nil?
      session[:show_diffs]=true
    end
    @show_diffs=session[:show_diffs]
  end
end
