require "yaml"
require "uri"
require "./processor"
require "./registry"
require "./fabric"
require "./server"
require "./layout_store"

# Register the Zstd codec with crystal-avro before any processor
# loads — OCF decoders may see codec="zstandard" on the first tick.
require "./avro_zstd_adapter"

require "./processors/all"

def load_config(path : String) : YAML::Any
  YAML.parse(File.read(path))
end

def build_fabric_from(config : YAML::Any) : Fabric
  fabric = Fabric.new
  nodes = config["nodes"].as_a
  nodes.each do |n|
    cfg_yaml = n["config"]?.try(&.as_h?) || {} of YAML::Any => YAML::Any
    cfg = {} of String => String
    cfg_yaml.each { |k, v| cfg[k.as_s] = v.as_s? || v.raw.to_s }
    fabric.add(n["name"].as_s, n["type"].as_s, cfg)
  end
  nodes.each do |n|
    routes = n["routes"]?.try(&.as_h?)
    next if routes.nil?
    routes.each do |rel, targets|
      targets.as_a.each do |t|
        fabric.connect(n["name"].as_s, rel.as_s, t.as_s)
      end
    end
  end
  fabric.mark_saved
  fabric
end

config_path = ARGV[0]? || "config.yml"
STDOUT.puts "[boot] zinc-flow (Crystal #{Crystal::VERSION})"
STDOUT.puts "[boot] config: #{config_path}"

cfg = load_config(config_path)
fabric = build_fabric_from(cfg)
if cycle = fabric.find_cycle
  STDERR.puts "[boot] DAG cycle detected: #{cycle}"
  STDERR.puts "[boot] zinc-flow requires an acyclic processor graph — break the cycle in config and restart."
  exit 1
end
STDOUT.puts "[boot] fabric: #{fabric.nodes.size} nodes · #{Registry.metas.size} processor types registered"

tick_ms = cfg["tick_interval_ms"]?.try(&.as_i?) || 1000
port = cfg["port"]?.try(&.as_i?) || 9092
ui_root = cfg["ui_root"]?.try(&.as_s?) || ENV["ZINC_UI_ROOT"]?
layout_path = cfg["layout_path"]?.try(&.as_s?) || "layout.json"
layout = LayoutStore.new(layout_path)

# Background tick loop. Each iteration spawns a fiber so a slow source
# doesn't push the next tick out of its slot.
spawn do
  loop do
    sleep tick_ms.milliseconds
    spawn { fabric.tick_sources }
  end
end

ApiServer.new(fabric, layout, port, ui_root).start
