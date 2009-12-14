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
      sp=$2
      val=$1
      if $2 !~ /[hdwmHDWM]/
        sp=""
      end
      time = case sp.downcase
             when 'h'
               val.to_f.hours.ago
             when 'd'
               val.to_f.days.ago
             when 'w'
               val.to_f.weeks.ago
             when 'm'
               val.to_f.minutes.ago
             else
               val.to_f.seconds.ago
             end
    elsif !str.nil?
      time=Time.parse(str) # Try Time.parse.
      # Will match things like: Nov/13, Aug-20, etc.
      # Nothing to lose since above didn't work.
    end
    time
  end
end
