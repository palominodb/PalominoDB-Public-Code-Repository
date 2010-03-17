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
require 'active_record'
require 'ttt/format/text'


module TTT
  class EmailFormatter < Formatter
    runner_for :email
    class TttMailer < ActionMailer::Base
      def report(to, sender, subj, bdy)
        recipients to
        from sender
        subject subj
        body bdy
      end
    end
    def format_defn(stream, rows, *args)
      link_type=false
      link_url=nil
      # If include_links is set, then we'll Need the gui_url.
      # link_type should be one of 'true', 'false', or 'html'
      # Anything else is treated as 'true'
      if link_type=want_option('include_links', false)
        link_url=need_option('gui_url')
      end
      last_run=nil
      reject_ignores(rows).each do |r|
        if last_run!=r.run_time
          stream.puts "--- #{r.run_time}"
          last_run=r.run_time
        end
        if link_type == 'text'
          stream.puts "#{r.status}\t#{r.server}.#{r.database_name}.#{r.table_name}\t<#{link_url}/servers/#{r.server}/databases/#{r.database_name}/tables/#{r.table_name}?show_diff=true&at=#{r.run_time.to_i}>"
        elsif link_type == 'html'
          stream.puts %Q{#{r.status}\t<a href="#{link_url}/servers/#{r.server}/databases/#{r.database_name}/tables/#{r.table_name}?show_diff=true&at=#{r.run_time.to_i}">#{r.server}.#{r.database_name}.#{r.table_name}</a>}
        else
          stream.puts "#{r.status}\t#{r.server}.#{r.database_name}.#{r.table_name}"
        end
      end
    end

    def format_user(stream, rows, *args)
      last_run=nil
      rows.each do |r|
        if last_run!=r.run_time
          stream.puts "--- #{r.run_time}"
          stream.puts "host        grant"
          last_run=r.run_time
        end
        stream.puts "#{r.server}        #{r.to_s}"
      end
    end

    def format(rows, *args)
      opts=args.extract_options!
      if ! cfg.key? "formatter_options" and ! cfg["formatter_options"].key? "email"
        stream.puts "[error]: Need email formatter options set to send email!"
        return false
      end

      changes=0
      reject_ignores(rows).each do |row|
        if row.tchanged? then
          changes +=1
        end
      end

      if cfg["formatter_options"]["email"].key? "send_empty"
        if !cfg["formatter_options"]["email"]["send_empty"] and changes==0
          return true
        end
      end
      tstream=StringIO.new
      if [TTT::TableDefinition, TTT::TableView].include? rows[0].class
        format_defn(tstream, rows, args)
      elsif rows[0].class == TTT::TableUser
        format_user(tstream, rows, args)
      else
        raise "Unable to handle this record type: #{rows[0].class.to_s}."
      end

      #TextFormatter.new(tstream, cfg).format(rows, opts)
      subj_prefix=case cfg["formatter_options"]["email"].key? "subjectprefix"
                  when true
                    cfg["formatter_options"]["email"]["subjectprefix"]
                  when false
                    "[TTT] "
                  end
      if !cfg["formatter_options"]["email"].key? "emailto"
        tstream.puts "[error]: Need 'formatter_options.email.emailto' to send email!"
        return false
      end
      if cfg["formatter_options"]["email"].key? "delivery_method"
        ActionMailer::Base.delivery_method=cfg["formatter_options"]["email"]["delivery_method"].to_sym
      else
        ActionMailer::Base.delivery_method=:sendmail
      end
      TttMailer.deliver_report(cfg["formatter_options"]["email"]["emailto"], "ttt@#{`hostname`}", subj_prefix + "#{rows[0].collector.to_s} changes: #{changes}", tstream.string)
      true
    end
  end
end
