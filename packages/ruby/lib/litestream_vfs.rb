require "rbconfig"

module LitestreamVfs
  EXT_MAP = {
    "linux" => "litestream-vfs.so",
    "darwin" => "litestream-vfs.dylib",
  }.freeze

  def self.loadable_path
    os = RbConfig::CONFIG["host_os"]
    key = case os
          when /linux/  then "linux"
          when /darwin/i then "darwin"
          else raise "Unsupported platform: #{os}"
          end

    filename = EXT_MAP.fetch(key)
    path = File.join(__dir__, filename)
    raise "VFS extension not found at #{path}" unless File.exist?(path)
    path
  end

  def self.load(db)
    db.enable_load_extension(true)
    db.load_extension(loadable_path.delete_suffix(File.extname(loadable_path)))
    db.enable_load_extension(false)
  end
end
