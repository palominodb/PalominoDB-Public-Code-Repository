
Then /^validate raises SemanticsError with type (\w+)$/ do |type|
  no_exception=true
  begin
  @dsn.validate
  rescue Exception => e
    no_exception=false
    unless e.class == Pdb::SemanticsError
      raise
    end
    unless e.type == type.to_sym
      raise e, "Must be of #{type} type. Got: #{e.type}"
    end
  end
  raise Exception, "Did not catch SemanticsError with type #{type}" if no_exception
end
