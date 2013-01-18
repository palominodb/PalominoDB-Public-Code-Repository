# dsn.rb
# Copyright (C) 2013 PalominoDB, Inc.
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

require 'lib/pdb/dsn'

Given /^an empty dsn$/ do
  Given "a dsn from file empty_dsn.yml"
end

Given /^a dsn from file (.*)$/ do |file|
  @dsn = Pdb::DSN.new("features/files/#{file}")
end

Transform /^should raise (\w+)$/ do |step_arg|
  exp_class=Pdb.const_get /(\w+)$/.match(step_arg)[0]
  if exp_class.ancestors.include? Exception
    exp_class
  else
    Exception
  end
end

Then /^validate (should raise \w+)/ do |type|
  begin
    @dsn.validate
  rescue Exception => e
    unless e.class == type
      raise
    end
  end
end

Then /^validate should pass$/ do
  @dsn.validate
end

And /^getting the key (\w+) from server (\w+) should return (.+)$/ do |key,srv,val|
  real_val=@dsn.send "server_#{key}", srv
  unless real_val == val
    raise Exception, "Did not get the expected result '#{val}', got: '#{real_val}'"
  end
end
