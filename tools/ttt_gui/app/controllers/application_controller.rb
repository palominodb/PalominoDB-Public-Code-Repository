# Filters added to this controller apply to all controllers in the application.
# Likewise, all the methods added will be available for all controllers.

require 'ttt/collector'

class ApplicationController < ActionController::Base
  helper :all # include all helpers, all the time
  protect_from_forgery # See ActionController::RequestForgeryProtection for details
  before_filter :setup_since_string

  def str_to_time(str)
    time=nil
    if !str.nil? and str =~ /(\d+(?:\.?\d+)?)([hdwm])?/
      logger.debug "str_to_time: got valid time sting"
      time = case $2
             when 'h'
               $1.to_f.hours.ago
             when 'd'
               $1.to_f.days.ago
             when 'w'
               $1.to_f.weeks.ago
             when 'm'
               $1.to_f.minutes.ago
             else
               $1.to_f.seconds.ago
             end
    end
    logger.debug"str_to_time: #{time}"
    time
  end

  def last_collector_run(s)
    TTT::Collector.get_last_run(s)
  end


  private
  def setup_since_string
    if params[:since]
      session[:since_string] = params[:since]
      @since_string=params[:since]
    elsif session[:since_string]
      @since_string=session[:since_string]
    elsif params[:since] == "last"
      session[:since_string] = nil
      @since_string=nil
    end
  end
end
