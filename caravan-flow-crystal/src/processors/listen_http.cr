require "http/server"
require "../processor"

# ListenHTTP — binds an HTTP listener on its own port and turns each
# POST body into a FlowFile. Headers prefixed X-Flow- become attributes
# with the prefix stripped.
#
# Lifecycle:
#   on_start      — spawns a background fiber running HTTP::Server.listen.
#                   Accepted requests push onto an internal queue.
#   source_tick   — fabric calls this on tick; drains the queue.
#   on_stop       — closes the server; fiber exits.
#
# The fabric + server are decoupled: HTTP accepts at any time but only
# enters the graph on the next tick, so backpressure is natural (queue
# grows if ticks are slow; we cap it).
class ListenHTTP < Processor
  property port : Int32 = 9100
  property path : String = "/"
  property max_body_bytes : Int32 = 16 * 1024 * 1024

  register "ListenHTTP",
    description: "Binds an HTTP listener; each POST body becomes a FlowFile",
    category: "Source",
    kind: "source",
    params: {
      port: {type: "Int32", required: true, placeholder: "9100"},
      path: {type: "String", default: "/", description: "path to listen on"},
      max_body_bytes: {
        type: "Int32", default: "16777216",
        description: "reject bodies larger than this with 413",
      },
    }

  QUEUE_CAP = 256

  @queue : Deque(FlowFile) = Deque(FlowFile).new
  @mutex : Mutex = Mutex.new
  @server : HTTP::Server? = nil

  def on_start : Nil
    normalized = @path.starts_with?("/") ? @path : "/#{@path}"
    server = HTTP::Server.new do |ctx|
      if ctx.request.method != "POST"
        ctx.response.status_code = 405
        next
      end
      unless ctx.request.path == normalized
        ctx.response.status_code = 404
        next
      end

      length = ctx.request.headers["Content-Length"]?.try(&.to_i?) || 0
      if length > @max_body_bytes
        ctx.response.status_code = 413
        next
      end

      body = ctx.request.body
      body_bytes = body ? body.gets_to_end.to_slice : Bytes.empty

      attrs = {"source" => "ListenHTTP", "listen.http.port" => @port.to_s}
      ctx.request.headers.each do |name, values|
        if name.starts_with?("X-Flow-")
          attrs[name[("X-Flow-".size)..].downcase] = values.first
        end
      end

      ff = FlowFile.new(content: body_bytes, attributes: attrs)

      accepted = @mutex.synchronize do
        if @queue.size >= QUEUE_CAP
          false
        else
          @queue.push(ff)
          true
        end
      end

      if accepted
        ctx.response.status_code = 202
      else
        ctx.response.status_code = 503
        ctx.response.print "ListenHTTP queue full (#{QUEUE_CAP} pending)"
      end
    end
    server.bind_tcp("0.0.0.0", @port)
    @server = server
    STDOUT.puts "[listen-http] :#{@port}#{normalized}"
    spawn do
      begin
        server.listen
      rescue e
        STDERR.puts "[listen-http] :#{@port} stopped: #{e.message}"
      end
    end
  end

  def on_stop : Nil
    @server.try(&.close)
    @server = nil
  end

  # No-op for process(); this is a source — the fabric never dispatches
  # into ListenHTTP, only pulls from source_tick.
  def process(ff : FlowFile) : Nil
  end

  def source_tick : Array(FlowFile)
    @mutex.synchronize do
      drained = @queue.to_a
      @queue.clear
      drained
    end
  end
end
