require 'digest/sha1'
class SlowQueriesController < ApplicationController
  class Filter
    @@type_types={}
    begin
      @@type_types=SqlProfilerHistory.columns_hash
    rescue ActiveRecord::StatementInvalid => ar_si
    end

    attr_reader :type, :value
    def initialize(from_hash = {'sample' => "", 'value' => ''})
      @type = from_hash['type']
      @value = from_hash['value']
      @matcher = op(@value)
    end

    def match?(val)
      @matcher.call(val)
    end

    def value=(o)
      @value = o
      @matcher = op(@value)
    end

    private
    def op(str)
      if str =~ /^\s*(==|\>=|\<=|=~)\s*(.*)$/ then
        unless $1 == '=~'
          b = @@type_types[@type].type_cast($2)
        else
          b = $2
        end
        case $1
        when '==' then Proc.new { |a| a == b }
        when '>=' then Proc.new { |a| a >= b }
        when '<=' then Proc.new { |a| a <= b }
        when '=~' then Proc.new { |a| a =~ Regexp.new(b) } # Can't typecast to a regexp, so... raw!
        end
      else
        Proc.new { |a| true }
      end
    end
  end
  before_filter :include_rqueries_session
  def index
    @filter = params[:filter] ? Filter.new(params[:filter]) : Filter.new()
    #@filter[:type] = params[:filter][:type]
    #@filter[:value] = params[:filter][:value]
    cond= @include_reviewed ? {} : {:conditions => ['reviewed_by IS NULL']}
    @queries=SqlProfilerQuery.all(cond)
    @queries=@queries.select { |q| q.last_seen >= since_t  }
    @query_histories={}
    SqlProfilerHistory.all(:conditions => ['checksum IN(?)', @queries.map { |q| q.checksum }], :order => 'ts_max ASC').map { |qh| @query_histories[qh.checksum] = qh }
    @queries.reject! do |q|
      !@filter.match? @query_histories[q.checksum][@filter.type]
    end
  end

  def show
    @host=nil
    if params[:server_id]
      logger.debug "Rendering only for server: #{params[:server_id]}"
      @host=SqlProfilerHost.find_by_host(params[:server_id])
      host_queries=@host.queries(@include_reviewed)
      cond= @include_reviewed ? {:conditions => ['checksum IN (?)', host_queries.map { |hq| hq.checksum }]} : {:conditions => ['reviewed_by IS NULL AND checksum IN (?)', host_queries.map { |hq| hq.checksum }]}
      @queries=SqlProfilerQuery.all(cond)
    elsif params[:id]
      @queries=[SqlProfilerQuery.find(params[:id].to_i)]
    end
    if !@queries.nil? and @queries.length != 0
      @query_histories={}
      @queries.each { |q| @query_histories[q.checksum] = q.get_history }
    elsif @queries.nil? or @queries.length == 0
      render :action => :no_slow_queries
    end
  end

  def edit
    @query=SqlProfilerQuery.find(params[:id].to_i)
  end


  private
  def include_rqueries_session
    if params[:include_reviewed]
      session[:include_reviewed] = params[:include_reviewed].to_i == 1
    elsif params[:include_reviewed].nil? and session[:include_reviewed].nil?
      session[:include_reviewed]=false
    end
    @include_reviewed=session[:include_reviewed]
  end

end
