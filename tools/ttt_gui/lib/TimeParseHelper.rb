require 'time'
module TimeParseHelper
  def since_str
    @since_string
  end
  def since_t
    str_to_time(@since_string)
  end
  def str_to_time(str)
    time=nil
    if !str.nil? and str =~ /^\s*(\d+(?:\.?\d+)?)([hdwmHDWM])?/
      time = case $2.downcase
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
    elsif !str.nil?
      time=Time.parse(str) # Try Time.parse.
      # Will match things like: Nov/13, Aug-20, etc.
      # Nothing to lose since above didn't work.
    end
    time
  end
end
