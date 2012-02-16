class SqlProfilerQueryController < ApplicationController
  def update
    sqr=SqlProfilerQuery.find(params[:id])
    sqr.reviewed_by=params[:sql_profiler_query]['reviewed_by']
    sqr.comments=params[:sql_profiler_query]['comments']
    sqr.reviewed_on=Time.now
    sqr.save
    redirect_to :slow_queries
  end
end
