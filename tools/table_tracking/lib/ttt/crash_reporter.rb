# Copyright (c) 2009-2010, PalominoDB, Inc.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
# 
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
# 
#   * Neither the name of PalominoDB, Inc. nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
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
