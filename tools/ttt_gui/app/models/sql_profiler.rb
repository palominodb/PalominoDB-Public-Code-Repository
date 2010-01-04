require 'active_record'

begin
class SqlProfiler < ActiveRecord::Base
  self.abstract_class = true
  establish_connection(
    :adapter  => "mysql",
    :host     => TTT_CONFIG['gui_options']['slow_query_host'],
    :username => TTT_CONFIG['gui_options']['slow_query_user'],
    :password => TTT_CONFIG['gui_options']['slow_query_pass'],
    :database => TTT_CONFIG['gui_options']['slow_query_schema'],
    :socket   => TTT_CONFIG['gui_options']['slow_query_socket']
  )
end
rescue NoMethodError => nme
end
