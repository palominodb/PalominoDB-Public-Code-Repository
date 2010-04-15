# Methods added to this helper will be available to all templates in the application.
require 'TimeParseHelper'
require 'diff/lcs'
require 'diff/lcs/hunk'
module ApplicationHelper
  include TimeParseHelper

  Time::DATE_FORMATS[:slong] =
    lambda do |time|
      if time <= 1.year.ago
        time.to_formatted_s(:long)
      else
        time.to_formatted_s(:short)
      end
    end

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
      cur_create=cur.created_at || cur.run_time
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


end
