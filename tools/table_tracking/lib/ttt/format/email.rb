require 'rubygems'
require 'actionmailer'
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
    def format(rows, *args)
      opts=args.extract_options!
      if ! cfg.key? "formatter_options" and ! cfg["formatter_options"].key? "email"
        stream.puts "[error]: Need email formatter options set to send email!"
        return false
      end

      link_type=false
      link_url=nil
      # If include_links is set, then we'll Need the gui_url.
      # link_type should be one of 'true', 'false', or 'html'
      # Anything else is treated as 'true'
      if link_type=want_option('include_links', false)
        link_url=need_option('gui_url')
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

      last_run=nil
      reject_ignores(rows).each do |r|
        if last_run!=r.run_time
          tstream.puts "--- #{r.run_time}"
          last_run=r.run_time
        end
        if link_type == 'text'
          tstream.puts "#{r.status}\t#{r.server}.#{r.database_name}.#{r.table_name}\t<#{link_url}/servers/#{r.server}/databases/#{r.database_name}/tables/#{r.table_name}?show_diff=true&at=#{r.run_time.to_i}>"
        elsif link_type == 'html'
          tstream.puts %Q{#{r.status}\t<a href="#{link_url}/servers/#{r.server}/databases/#{r.database_name}/tables/#{r.table_name}?show_diff=true&at=#{r.run_time.to_i}">#{r.server}.#{r.database_name}.#{r.table_name}</a>}
        else
          tstream.puts "#{r.status}\t#{r.server}.#{r.database_name}.#{r.table_name}"
        end
      end
      #TextFormatter.new(tstream, cfg).format(rows, opts)
      subj_prefix=case cfg["formatter_options"]["email"].key? "subjectprefix"
                  when true
                    cfg["formatter_options"]["email"]["subjectprefix"]
                  when false
                    "[TTT] "
                  end
      if !cfg["formatter_options"]["email"].key? "emailto"
        stream.puts "[error]: Need 'formatter_options.email.emailto' to send email!"
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
