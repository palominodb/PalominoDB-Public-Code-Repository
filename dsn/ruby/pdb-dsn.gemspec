spec = Gem::Specification.new do |s|
  s.name = "pdb-dsn"
  s.version = "0.2.6"
  s.author = "PalominoDB Engineering"
  s.email = "eng@palominodb.com"
  s.homepage = "http://palominodb.com"
  s.platform = Gem::Platform::RUBY
  s.summary = "API for PalominoDB DSN"
  s.description =<<E_DESC
The reference implementation of an API for validating and
interacting with PalominoDB DSN.yml files.
E_DESC
  s.files = ["lib/pdb/dsn.rb", "bin/pdb-dsn-validate"]
  s.executables = [ "pdb-dsn-validate" ]
end
