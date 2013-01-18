# crash_reporter.rb
# Copyright (C) 2009-2013 PalominoDB, Inc.
# 
# You may contact the maintainers at eng@palominodb.com.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

require 'rubygems'
require 'action_mailer'

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
          attachment :content_type => 'text/yaml',
            :filename => "crash_error#{i}.yaml",
            :body => $!.to_yaml + "\n" + $!.backtrace.to_yaml
        end
        i += 1
      end
    end
  end # class
  CrashMailer.delivery_method = :sendmail
end
