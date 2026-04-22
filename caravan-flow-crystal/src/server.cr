require "http/server"
require "json"
require "uri"
require "./fabric"
require "./registry"
require "./provenance"
require "./edge_stats"
require "./layout_store"
require "./expression"

# Full management API + UI static serving. Shapes match
# caravan-csharp's sibling so the React UI in caravan-flow-ui-web
# can drive this worker too.
class ApiServer
  def initialize(@fabric : Fabric, @layout : LayoutStore, @port : Int32 = 9092, @ui_root : String? = nil)
  end

  def start : Nil
    server = HTTP::Server.new { |ctx| handle(ctx) }
    server.bind_tcp("0.0.0.0", @port)
    STDOUT.puts "[server] listening on :#{@port}"
    STDOUT.puts "[ui] serving from #{@ui_root}" if @ui_root
    server.listen
  end

  private def handle(ctx : HTTP::Server::Context) : Nil
    path = ctx.request.path
    method = ctx.request.method
    ctx.response.headers["Access-Control-Allow-Origin"] = "*"
    ctx.response.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
    ctx.response.headers["Access-Control-Allow-Headers"] = "Content-Type"

    if method == "OPTIONS"
      ctx.response.status_code = 204
      return
    end

    dispatch(ctx, method, path)
  rescue e : Exception
    ctx.response.status_code = 500
    ctx.response.content_type = "application/json"
    {error: e.message || e.class.name}.to_json(ctx.response)
  end

  # ---------------------------------------------------------------------
  # Dispatcher. Ordered: static reads first, then POST/PUT/DELETE branches
  # matched on prefix. Not a full router — just a pragmatic switchboard
  # that keeps each endpoint inline + readable.
  # ---------------------------------------------------------------------
  private def dispatch(ctx, method : String, path : String) : Nil
    # --- reads ---
    if method == "GET"
      return json_out(ctx) { Registry.metas.to_json(ctx.response) } if path == "/api/registry"
      return json_out(ctx) { @fabric.flow.to_json(ctx.response) } if path == "/api/flow"
      return json_out(ctx) { stats_body(ctx) } if path == "/api/stats"
      return json_out(ctx) { @fabric.stats_map.to_json(ctx.response) } if path == "/api/processor-stats"
      return json_out(ctx) { flow_status_body(ctx) } if path == "/api/flow/status"
      return json_out(ctx) { EdgeStats.snapshot.to_json(ctx.response) } if path == "/api/edge-stats"
      return json_out(ctx) { sources_body(ctx) } if path == "/api/sources"
      return json_out(ctx) { overlays_body(ctx) } if path == "/api/overlays"
      return json_out(ctx) { {enabled: false}.to_json(ctx.response) } if path == "/api/vc/status"
      return json_out(ctx) { {positions: @layout.get, path: @layout.path}.to_json(ctx.response) } if path == "/api/layout"
      return health(ctx) if path == "/health"
      return readyz(ctx) if path == "/readyz"
      return prometheus(ctx) if path == "/metrics"

      if path == "/api/provenance"
        n = param(ctx, "n").try(&.to_i?) || 100
        return json_out(ctx) { Provenance.recent(n).to_json(ctx.response) }
      end

      if path == "/api/provenance/failures"
        n = param(ctx, "n").try(&.to_i?) || 50
        return json_out(ctx) { Provenance.failures(n).to_json(ctx.response) }
      end

      if path.starts_with?("/api/provenance/")
        id = path["/api/provenance/".size..]
        return json_out(ctx) { Provenance.by_id(id).to_json(ctx.response) }
      end

      if path.starts_with?("/api/processors/") && path.ends_with?("/samples")
        name = URI.decode path["/api/processors/".size...(path.size - "/samples".size)]
        return json_out(ctx) { samples_body(ctx, name) }
      end

      # UI static fallback.
      root = @ui_root
      if root
        serve_static(ctx, root, path)
        return
      end

      not_found(ctx, path)
      return
    end

    # --- writes ---
    body_json = read_body(ctx)

    if method == "POST"
      case path
      when "/api/flow/save"
        @fabric.mark_saved
        return json_out(ctx) { {ok: true, note: "prototype: no git / disk persistence wired"}.to_json(ctx.response) }
      when "/api/reload"
        return json_out(ctx) { {ok: true, note: "prototype: reload not wired"}.to_json(ctx.response) }
      when "/api/processors/add"
        @fabric.add(body_json["name"].as_s, body_json["type"].as_s, coerce_config(body_json["config"]?))
        connections = body_json["connections"]?
        if connections
          connections.as_h.each do |rel, tgts|
            tgts.as_a.each { |t| @fabric.connect(body_json["name"].as_s, rel, t.as_s) }
          end
        end
        return ok(ctx)
      when "/api/processors/enable"
        @fabric.toggle(body_json["name"].as_s, true)
        return ok(ctx)
      when "/api/processors/disable"
        @fabric.toggle(body_json["name"].as_s, false)
        return ok(ctx)
      when "/api/connections"
        @fabric.connect(body_json["from"].as_s, body_json["relationship"].as_s, body_json["to"].as_s)
        return ok(ctx)
      when "/api/sources/start"
        @fabric.toggle(body_json["name"].as_s, true)
        return ok(ctx)
      when "/api/sources/stop"
        @fabric.toggle(body_json["name"].as_s, false)
        return ok(ctx)
      when "/api/sources/add"
        @fabric.add(body_json["name"].as_s, body_json["type"].as_s, coerce_config(body_json["config"]?))
        return ok(ctx)
      when "/api/expression/parse"
        return eval_expression(ctx, body_json)
      when "/api/layout"
        positions = {} of String => LayoutStore::Position
        body_json["positions"].as_h.each do |k, v|
          h = v.as_h
          positions[k] = LayoutStore::Position.new(h["x"].as_f, h["y"].as_f)
        end
        @layout.set(positions)
        return ok(ctx)
      when "/api/tick"
        emitted = @fabric.tick_sources
        return json_out(ctx) { {emitted: emitted}.to_json(ctx.response) }
      end

      if path.starts_with?("/api/processors/") && path.ends_with?("/stats/reset")
        name = URI.decode path["/api/processors/".size...(path.size - "/stats/reset".size)]
        @fabric.reset_stats(name)
        return ok(ctx)
      end

      not_found(ctx, path)
      return
    end

    if method == "PUT"
      if path.starts_with?("/api/processors/") && path.ends_with?("/config")
        name = URI.decode path["/api/processors/".size...(path.size - "/config".size)]
        type = body_json["type"]?.try(&.as_s?)
        @fabric.update_config(name, type, coerce_config(body_json["config"]?))
        return ok(ctx)
      end
      if path.starts_with?("/api/connections/")
        from = URI.decode path["/api/connections/".size..]
        rels = {} of String => Array(String)
        body_json.as_h.each { |k, v| rels[k] = v.as_a.map(&.as_s) }
        @fabric.set_connections(from, rels)
        return ok(ctx)
      end
      if path.starts_with?("/api/sources/") && path.ends_with?("/config")
        name = URI.decode path["/api/sources/".size...(path.size - "/config".size)]
        @fabric.update_config(name, nil, coerce_config(body_json))
        return ok(ctx)
      end
      not_found(ctx, path)
      return
    end

    if method == "DELETE"
      if path == "/api/processors/remove"
        nm = param(ctx, "name")
        @fabric.remove(nm) if nm
        return ok(ctx)
      end
      if path == "/api/connections"
        @fabric.disconnect(body_json["from"].as_s, body_json["relationship"].as_s, body_json["to"].as_s)
        return ok(ctx)
      end
      if path == "/api/sources/remove"
        nm = param(ctx, "name")
        @fabric.remove(nm) if nm
        return ok(ctx)
      end
      not_found(ctx, path)
      return
    end

    not_found(ctx, path)
  end

  # --- endpoint bodies ---

  private def flow_status_body(ctx) : Nil
    saved = @fabric.last_saved_counter.get
    cur = @fabric.mutation_counter.get
    body = {
      dirty:            saved != cur,
      mutationCounter:  cur,
      lastSavedCounter: saved,
      lastSavedAgoMs:   nil,
    }
    body.to_json(ctx.response)
  end

  # Fabric-wide stats mirroring caravan-csharp's GET /api/stats shape.
  # Active-execution tracking isn't wired in Crystal (dispatch fire-and-
  # forgets through `spawn`), so the count is reported as 0 — keep the
  # key so the UI renders without a conditional.
  private def stats_body(ctx) : Nil
    total = 0_i64
    source_count = 0
    @fabric.nodes.each_value do |n|
      total += n.processed.get
      meta = Registry.metas.find { |m| m.name == n.type }
      source_count += 1 if meta.try(&.kind) == "source"
    end
    {
      processed:         total,
      activeExecutions:  0,
      processors:        @fabric.nodes.size,
      sources:           source_count,
    }.to_json(ctx.response)
  end

  # Liveness — process is up, sources listed but no readiness gating.
  # K8s livenessProbe target.
  private def health(ctx) : Nil
    sources = [] of NamedTuple(name: String, type: String, running: Bool)
    @fabric.nodes.each_value do |n|
      meta = Registry.metas.find { |m| m.name == n.type }
      next unless meta.try(&.kind) == "source"
      sources << {name: n.name, type: n.type, running: n.enabled?}
    end
    ctx.response.content_type = "application/json"
    {status: "healthy", sources: sources}.to_json(ctx.response)
  end

  # Readiness — process alive + ≥1 processor + every source running.
  # Returns 503 when not ready so k8s gates service traffic.
  private def readyz(ctx) : Nil
    proc_count = @fabric.nodes.size
    sources = [] of NamedTuple(name: String, type: String, running: Bool)
    not_running = [] of String
    @fabric.nodes.each_value do |n|
      meta = Registry.metas.find { |m| m.name == n.type }
      next unless meta.try(&.kind) == "source"
      sources << {name: n.name, type: n.type, running: n.enabled?}
      not_running << n.name unless n.enabled?
    end
    ready = proc_count > 0 && not_running.empty?
    ctx.response.status_code = ready ? 200 : 503
    ctx.response.content_type = "application/json"
    {
      ready:              ready,
      processors:         proc_count,
      sourcesTotal:       sources.size,
      sourcesNotRunning:  not_running,
    }.to_json(ctx.response)
  end

  private def sources_body(ctx) : Nil
    sources = [] of NamedTuple(name: String, type: String, running: Bool)
    @fabric.nodes.each_value do |n|
      meta = Registry.metas.find { |m| m.name == n.type }
      next if meta.nil?
      next unless meta.kind == "source"
      sources << {name: n.name, type: n.type, running: n.enabled?}
    end
    sources.to_json(ctx.response)
  end

  private def overlays_body(ctx) : Nil
    payload = {
      layers: [
        {role: "base", present: true, path: "config.yml", size: 0},
        {role: "local", present: false, path: nil, size: 0},
      ],
    }
    payload.to_json(ctx.response)
  end

  private def samples_body(ctx, name : String) : Nil
    node = @fabric.nodes[name]?
    if node.nil?
      {name: name, sampling: true, samples: [] of SampleRing::Sample}.to_json(ctx.response)
    else
      {name: name, sampling: true, samples: node.samples.snapshot}.to_json(ctx.response)
    end
  end

  private def eval_expression(ctx, body : JSON::Any) : Nil
    ctx.response.content_type = "application/json"
    src = body["expression"].as_s
    attrs = {} of String => String
    ctx_obj = body["context"]?
    if ctx_obj
      a = ctx_obj["attributes"]?
      if a
        a.as_h.each do |k, v|
          sv = v.as_s?
          attrs[k] = sv ? sv : v.to_s
        end
      end
    end
    rec : Hash(String, JSON::Any)? = nil
    if ctx_obj
      r = ctx_obj["record"]?
      rec = r.try(&.as_h?)
    end
    begin
      node = Expr::Parser.parse(src)
      v = node.eval(Expr::Context.new(attrs, rec))
      {ok: true, parse: "ok", eval: "ok", kind: v.class.name.downcase, value: v.to_s}.to_json(ctx.response)
    rescue e
      {ok: false, error: e.message || e.class.name, parse: "error"}.to_json(ctx.response)
    end
  end

  # --- helpers ---

  private def ok(ctx) : Nil
    json_out(ctx) { {ok: true}.to_json(ctx.response) }
  end

  private def json_out(ctx, &) : Nil
    ctx.response.content_type = "application/json"
    yield
  end

  private def not_found(ctx, path : String) : Nil
    ctx.response.status_code = 404
    ctx.response.content_type = "application/json"
    {error: "not found", path: path}.to_json(ctx.response)
  end

  private def param(ctx, name : String) : String?
    ctx.request.query_params[name]?
  end

  private def read_body(ctx) : JSON::Any
    body = ctx.request.body
    raw_text = body ? body.gets_to_end : "{}"
    if raw_text.empty?
      raw_text = "{}"
    end
    JSON.parse(raw_text)
  end

  private def coerce_config(src : JSON::Any?) : Hash(String, String)
    out = {} of String => String
    src.try &.as_h.try &.each do |k, v|
      out[k] = v.to_s
    end
    out
  end

  private def prometheus(ctx) : Nil
    ctx.response.content_type = "text/plain; version=0.0.4"
    @fabric.stats.each do |s|
      ctx.response.puts "caravan_processed_total{node=\"#{s[:name]}\",type=\"#{s[:type]}\",state=\"#{s[:state]}\"} #{s[:processed]}"
      ctx.response.puts "caravan_errors_total{node=\"#{s[:name]}\",type=\"#{s[:type]}\"} #{s[:errors]}"
    end
    EdgeStats.snapshot.each do |k, v|
      parts = k.split('|', 3)
      from = parts[0]? || ""
      rel = parts[1]? || ""
      to = parts[2]? || ""
      ctx.response.puts "caravan_edge_processed_total{from=\"#{from}\",relationship=\"#{rel}\",to=\"#{to}\"} #{v[:processed]}"
    end
  end

  private def serve_static(ctx, root : String, path : String) : Nil
    # Strip leading slash so File.join doesn't drop root (Crystal's
    # File.join treats a leading-/ second arg as absolute).
    rel = path == "/" ? "index.html" : path.lstrip('/')
    abs = File.join(root, rel)
    unless File.exists?(abs) && File.file?(abs)
      abs = File.join(root, "index.html")
    end
    unless File.exists?(abs)
      not_found(ctx, path)
      return
    end
    ctx.response.status_code = 200
    ctx.response.content_type = guess_mime(abs)
    File.open(abs) { |f| IO.copy(f, ctx.response) }
  end

  private def guess_mime(path : String) : String
    case File.extname(path)
    when ".html" then "text/html; charset=utf-8"
    when ".css"  then "text/css"
    when ".js"   then "application/javascript"
    when ".json" then "application/json"
    when ".svg"  then "image/svg+xml"
    when ".png"  then "image/png"
    when ".jpg"  then "image/jpeg"
    when ".jpeg" then "image/jpeg"
    when ".ico"  then "image/x-icon"
    when ".woff" then "font/woff"
    when ".woff2" then "font/woff2"
    else              "application/octet-stream"
    end
  end
end
