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
