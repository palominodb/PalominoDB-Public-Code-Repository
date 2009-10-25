module DashboardHelper
  def str_to_time(str)
      puts "In str_to_time"
      time=nil
      if !str.nil? and str =~ /(\d+(?:\.?\d+)?)([hdwm])?/
        debug "is valid time sting"
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
      puts "time: #{time}"
    time
  end
end
