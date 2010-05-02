require 'rubygems'
require 'action_mailer'
require 'ruby-debug'

module TTT
  class CrashMailer < ActionMailer::Base
    CRASH_TO    = 'brian@palominodb.com'
    CRASH_FROM  = "ttt-crash@#{`hostname -f`}"
    @@captured_ARGV = ARGV.dup
    def crash(excp, *args)
      recipients CRASH_TO
      from CRASH_FROM
      body "Attached is a YAML dump of the collected information."
      subject "TTT Crashed"
      attachment :content_type => 'text/yaml',
                 :filename => "exception_and_argv.yaml",
                 :body => excp.to_yaml + "\n" + excp.backtrace.to_yaml + "\n" + ['Program ARGV', $0, @@captured_ARGV].to_yaml
      i=1
      args.each do |a|
        begin
          attachment :content_type => 'text/yaml',
            :filename => "data#{i}.yaml",
            :body => a.to_yaml
        rescue
          debugger
          attachment :content_type => 'text/yaml',
            :filename => "crash_error#{i}.yaml",
            :body => $!.to_yaml + "\n" + $!.backtrace.to_yaml
        end
        i += 1
      end
    end
  end # class
end
