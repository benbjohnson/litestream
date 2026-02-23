Gem::Specification.new do |s|
  s.name        = "litestream-vfs"
  s.version     = ENV.fetch("LITESTREAM_VERSION", "0.0.0")
  s.summary     = "Litestream VFS extension for SQLite"
  s.description = "Bundles the Litestream VFS shared library for loading into SQLite connections."
  s.homepage    = "https://github.com/benbjohnson/litestream"
  s.license     = "Apache-2.0"
  s.authors     = ["Ben Johnson"]

  s.platform = Gem::Platform.new(ENV.fetch("PLATFORM", RUBY_PLATFORM))

  s.files = Dir["lib/**/*.rb"] + Dir["lib/**/*.so"] + Dir["lib/**/*.dylib"]
  s.require_paths = ["lib"]

  s.required_ruby_version = ">= 2.7"
end
