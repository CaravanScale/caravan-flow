require "./processor"
require "./registry"
require "./provenance"
require "./edge_stats"
require "./sample_ring"

# Fabric: the running graph. Each Node owns one processor, its
# config, outbound relationships → target-node-name map, stats,
# state (ENABLED | STOPPED), and a sample ring. Dispatching a FlowFile
# into a node runs its `process`, drains the outbox, pushes each
# emitted (rel, ff) pair onto every downstream target via fibers
# (parallelized when built with -Dpreview_mt).
class Fabric
  enum NodeState
    ENABLED
    STOPPED
  end

  class Node
    property name : String
    property type : String
    property processor : Processor
    property config : Hash(String, String)
    property routes : Hash(String, Array(String))
    property processed : Atomic(Int64)
    property errors : Atomic(Int64)
    property state : NodeState
    property samples : SampleRing

    def initialize(@name, @type, @processor, @config)
      @routes = {} of String => Array(String)
      @processed = Atomic(Int64).new(0_i64)
      @errors = Atomic(Int64).new(0_i64)
      @state = NodeState::ENABLED
      @samples = SampleRing.new
    end

    def enabled? : Bool
      @state == NodeState::ENABLED
    end

    def reset_stats : Nil
      @processed.set(0_i64)
      @errors.set(0_i64)
    end
  end

  getter nodes : Hash(String, Node)
  getter mutation_counter : Atomic(Int64)
  getter last_saved_counter : Atomic(Int64)

  def initialize
    @nodes = {} of String => Node
    @mutex = Mutex.new
    @mutation_counter = Atomic(Int64).new(0_i64)
    @last_saved_counter = Atomic(Int64).new(0_i64)
  end

  private def bump_mutation
    @mutation_counter.add(1_i64)
  end

  def add(name : String, type : String, config : Hash(String, String)) : Node
    @mutex.synchronize do
      raise "node exists: #{name}" if @nodes.has_key?(name)
      proc = Registry.create(type, config)
      node = Node.new(name, type, proc, config)
      @nodes[name] = node
      bump_mutation
      node
    end
  end

  def remove(name : String) : Nil
    @mutex.synchronize do
      node = @nodes[name]?
      return if node.nil?
      node.processor.on_stop
      @nodes.delete(name)
      # Drop any routes targeting this node from other nodes.
      @nodes.each_value do |n|
        n.routes.each { |rel, tgts| tgts.delete(name) }
        n.routes.reject! { |_, tgts| tgts.empty? }
      end
      bump_mutation
    end
  end

  def update_config(name : String, type : String?, config : Hash(String, String)) : Node
    @mutex.synchronize do
      node = @nodes[name]? || raise "no such node: #{name}"
      effective_type = type || node.type
      node.processor.on_stop
      new_proc = Registry.create(effective_type, config)
      node.processor = new_proc
      node.type = effective_type
      node.config = config
      bump_mutation
      node
    end
  end

  def toggle(name : String, enabled : Bool) : Node
    @mutex.synchronize do
      node = @nodes[name]? || raise "no such node: #{name}"
      node.state = enabled ? NodeState::ENABLED : NodeState::STOPPED
      bump_mutation
      node
    end
  end

  def connect(from : String, relationship : String, to : String) : Nil
    @mutex.synchronize do
      src = @nodes[from]? || raise "no such node: #{from}"
      @nodes[to]? || raise "no such node: #{to}"
      rel = (src.routes[relationship] ||= [] of String)
      rel << to unless rel.includes?(to)
      bump_mutation
    end
  end

  def disconnect(from : String, relationship : String, to : String) : Nil
    @mutex.synchronize do
      src = @nodes[from]? || return
      tgts = src.routes[relationship]?
      next if tgts.nil?
      tgts.delete(to)
      src.routes.delete(relationship) if tgts.empty?
      bump_mutation
    end
  end

  def set_connections(from : String, relationships : Hash(String, Array(String))) : Nil
    @mutex.synchronize do
      src = @nodes[from]? || raise "no such node: #{from}"
      src.routes.clear
      relationships.each do |rel, tgts|
        src.routes[rel] = tgts.dup
      end
      bump_mutation
    end
  end

  def reset_stats(name : String) : Nil
    @mutex.synchronize do
      node = @nodes[name]? || raise "no such node: #{name}"
      node.reset_stats
    end
  end

  def mark_saved : Nil
    @last_saved_counter.set(@mutation_counter.get)
  end

  # Run one node's process on one flowfile and fan out the outbox.
  # Skipped when the node is disabled (drop-on-the-floor — the flowfile
  # is ack'd into oblivion, matching the C# STOPPED semantics).
  def dispatch(node : Node, ff : FlowFile) : Nil
    return unless node.enabled?
    begin
      node.processor.process(ff)
      node.processed.add(1_i64)
    rescue e
      node.errors.add(1_i64)
      Provenance.record(ff.uuid, "FAILURE", node.name, e.message || e.class.name)
      return
    end
    emitted = node.processor.drain_outbox
    emitted.each do |rel, out_ff|
      node.samples.capture(out_ff)
      Provenance.record(out_ff.uuid, "ROUTE", node.name, rel)
      targets = node.routes[rel]?
      next if targets.nil?
      targets.each do |tname|
        target = @nodes[tname]?
        next if target.nil?
        EdgeStats.bump(node.name, rel, tname)
        spawn { dispatch(target, out_ff) }
      end
    end
  end

  # Tick all sources once. Source outputs go through the :success
  # relationship by convention.
  def tick_sources : Int32
    total = 0
    @nodes.each_value do |node|
      next unless node.enabled?
      produced = node.processor.source_tick
      next if produced.empty?
      produced.each do |ff|
        Provenance.record(ff.uuid, "CREATE", node.name)
        node.samples.capture(ff)
        targets = node.routes["success"]?
        if targets
          targets.each do |tname|
            target = @nodes[tname]?
            next if target.nil?
            EdgeStats.bump(node.name, "success", tname)
            spawn { dispatch(target, ff) }
          end
        end
        total += 1
      end
    end
    total
  end

  def stats : Array(NamedTuple(name: String, type: String, state: String, processed: Int64, errors: Int64))
    @nodes.values.map do |n|
      {name: n.name, type: n.type, state: n.state.to_s, processed: n.processed.get, errors: n.errors.get}
    end
  end

  # Shape the UI calls /api/flow for — processors + connections in one
  # payload matching caravan-csharp's Flow struct.
  def flow : Hash(String, JSON::Any)
    procs = @nodes.values.map do |n|
      {
        "name"        => n.name,
        "type"        => n.type,
        "state"       => n.state.to_s,
        "config"      => n.config,
        "connections" => n.routes,
        "stats"       => {"processed" => n.processed.get, "errors" => n.errors.get},
      }.to_json
    end
    sources = @nodes.values.select { |n| Registry.metas.find { |m| m.name == n.type }.try(&.kind) == "source" }
      .map { |n| {"name" => n.name, "type" => n.type, "running" => n.enabled?}.to_json }

    JSON.parse({
      "processors" => JSON.parse("[#{procs.join(",")}]"),
      "sources"    => JSON.parse("[#{sources.join(",")}]"),
    }.to_json).as_h
  end

  def stats_map : Hash(String, NamedTuple(processed: Int64, errors: Int64, state: String))
    result = {} of String => NamedTuple(processed: Int64, errors: Int64, state: String)
    @nodes.values.each do |n|
      result[n.name] = {processed: n.processed.get, errors: n.errors.get, state: n.state.to_s}
    end
    result
  end
end
