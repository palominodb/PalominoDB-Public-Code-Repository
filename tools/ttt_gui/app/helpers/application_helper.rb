# Methods added to this helper will be available to all templates in the application.
require 'TimeParseHelper'
module ApplicationHelper
  include TimeParseHelper

  def gen_diff(tbl)
    prev=tbl.previous_version
    prev_create=nil
    cur_create=nil
    cur=tbl

    if prev
      prev_create=prev.created_at
      if prev.create_syntax
        prev=prev.create_syntax.split("\n")
      else
        prev=[]
      end
    else
      prev=[]
      prev_create=Time.at(0)
    end

    if cur
      cur_create=cur.created_at
      if cur.create_syntax
        cur=tbl.create_syntax.split("\n")
      else
        cur_create=Time.at(0)
        cur=[]
      end
    else
      cur_create=cur.run_time
      cur=[]
    end

    diffs=Diff::LCS.diff(prev,cur)
    fld=0
    hunk=oldhunk=nil
    output=""
    output<<"--- #{tbl.table_name}\t#{prev_create.strftime('%Y-%m-%d %H:%M:%S %z')}\n"
    output<<"+++ #{tbl.table_name}\t#{cur_create.strftime('%Y-%m-%d %H:%M:%S %z')}\n"
    diffs.each do |p|
      begin
        hunk = Diff::LCS::Hunk.new(prev,cur, p, 3, fld)
        next unless oldhunk
        if hunk.overlaps? oldhunk
          hunk.unshift oldhunk
        else
          output<<oldhunk.diff(:unified)
        end
      ensure
        oldhunk=hunk
      end
    end

    output<<oldhunk.diff(:unified) if oldhunk

    output
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
