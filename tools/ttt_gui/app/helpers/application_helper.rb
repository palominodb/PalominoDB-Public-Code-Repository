# Methods added to this helper will be available to all templates in the application.
module ApplicationHelper
  def since_str
    @since_string
  end
  def since_t
    str_to_time(@since_string)
  end
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

  #def since_string
  #  if params[:since]
  #    session[:since_string] = params[:since]
  #    @since_string=params[:since]
  #  elsif session[:since_string]
  #    @since_string=session[:since_string]
  #  elsif params[:since] == "last"
  #    session[:since_string] = nil
  #    @since_string=nil
  #  end
  #  @since_string
  #end

  #def since_time
  #  str_to_time(@since_string)
  #end
end
