# Filters added to this controller apply to all controllers in the application.
# Likewise, all the methods added will be available for all controllers.

require 'ttt/collector'
require 'TimeParseHelper'

class ApplicationController < ActionController::Base
  include TimeParseHelper
  helper :all # include all helpers, all the time
  protect_from_forgery # See ActionController::RequestForgeryProtection for details
  before_filter :setup_since_string

  def last_collector_run(s)
    begin
      TTT::CollectorRun.find_by_collector(s.to_s).last_run
    rescue NoMethodError => e
      nil
    end
  end


  private
  def setup_since_string
    if params[:since]
      session[:since_string] = params[:since]
      @since_string=params[:since]
    elsif session[:since_string]
      @since_string=session[:since_string]
    else
      session[:since_string] = "24h"
      @since_string="24h"
    end
  end
end
