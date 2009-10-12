require 'rubygems'
require 'actionmailer'
require 'activerecord'
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
      if ! cfg.key? "email_options"
        stream.puts "[error]: Need 'email_options' to send email!"
        return false
      end

      changes=0
      reject_ignores(rows).each do |row|
        if row.tchanged? then
          changes +=1
        end
      end

      if cfg["email_options"].key? "send_empty"
        if !cfg["email_options"]["send_empty"] and changes==0
          return true
        end
      end
      tstream=StringIO.new
      TextFormatter.new(tstream, cfg).format(rows, opts)
      subj_prefix=case cfg["email_options"].key? "subjectprefix"
                  when true
                    cfg["email_options"]["subjectprefix"]
                  when false
                    "[TTT] "
                  end
      if !cfg["email_options"].key? "emailto"
        stream.puts "[error]: Need 'email_options.emailto' to send email!"
        return false
      end
      if cfg["email_options"].key? "delivery_method"
        ActionMailer::Base.delivery_method=cfg["email_options"]["delivery_method"].to_sym
      else
        ActionMailer::Base.delivery_method=:sendmail
      end
      TttMailer.deliver_report(cfg["email_options"]["emailto"], "ttt@#{`hostname`}", subj_prefix + "Changes: #{changes}", tstream.string)
      true
    end
  end
end
