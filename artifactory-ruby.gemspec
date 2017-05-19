lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require 'artifactory/version'
require 'date'

Gem::Specification.new do |s|
  s.name = 'artifactory-ruby'
  s.authors = ['Matteo Cerutti']
  s.date = Date.today.to_s
  s.description = 'Ruby client library for Kapacitor JSON REST API'
  s.email = 'matteo.cerutti@hotmail.co.uk'
  s.files = Dir.glob('{lib}/**/*') + %w(LICENSE README.md)
  s.require_paths = ["lib"]
  s.homepage = 'https://github.com/m4ce/artifactory-ruby'
  s.license = 'Apache 2.0'
  s.summary = 'Ruby client library that allows to interact with JFrog Artifactory REST API'
  s.version = Artifactory.version

  s.add_runtime_dependency 'json', '>= 1.7.0'
end
