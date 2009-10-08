spec = Gem::Specification.new do |s|
  s.name = "pdb-dsn"
  s.version = "0.1.0"
  s.author = "Brian Smith"
  s.email = "dba@palominodb.com"
  s.homepage = "http://blog.palominodb.com/blog/"
  s.platform = Gem::Platform::RUBY
  s.summary = "API for PalominoDB DSN"
  s.description =<<E_DESC
The reference implementation of an API for validating and
interacting with PalominoDB DSN.yml files.
E_DESC
  s.files = ["lib/pdb/dsn.rb"]
end
