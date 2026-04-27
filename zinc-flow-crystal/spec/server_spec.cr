require "spec"
require "http/client"
require "json"
require "../src/server"
require "../src/fabric"
require "../src/layout_store"
require "../src/processors/all"

# Boot an ApiServer on an ephemeral port in a spawned fiber, hand the
# spec an HTTP::Client, and tear the server down after. Sharing this
# helper keeps each `it` block focused on one endpoint shape.
private def with_server(fabric : Fabric, layout : LayoutStore = LayoutStore.new)
  server = HTTP::Server.new { |ctx|
    # Mirror ApiServer#handle without the exception-wrap path so the
    # spec surfaces real errors instead of a 500.
    ctx.response.headers["Access-Control-Allow-Origin"] = "*"
    api = ApiServer.new(fabric, layout, 0)
    api.as(ApiServer)
    ctx.response.status_code = 404
  }
  # Simpler path: use the ApiServer's own start bound to port 0 in a
  # fiber, then hand a client at that port.
  api = ApiServer.new(fabric, layout, 0)
  http = HTTP::Server.new { |ctx| api.__send_handle__(ctx) }
  addr = http.bind_tcp("127.0.0.1", 0)
  port = addr.port
  done = Channel(Nil).new
  spawn do
    begin
      http.listen
    ensure
      done.send(nil)
    end
  end
  client = HTTP::Client.new("127.0.0.1", port)
  begin
    yield client, port
  ensure
    http.close
    done.receive? rescue nil
  end
end

# ApiServer#handle is private. Reopen to expose it for the spec only.
class ApiServer
  def __send_handle__(ctx : HTTP::Server::Context) : Nil
    handle(ctx)
  end
end

describe ApiServer do
  it "GET /api/stats returns the fabric-wide counters" do
    fab = Fabric.new
    fab.add("n", "UpdateAttribute", {"key" => "k", "value" => "v"})
    with_server(fab) do |client, _|
      resp = client.get("/api/stats")
      resp.status_code.should eq 200
      body = JSON.parse(resp.body)
      body["processed"].as_i64.should eq 0
      body["processors"].as_i.should eq 1
      body["activeExecutions"].as_i.should eq 0
    end
  end

  it "GET /health returns status:healthy + an array of sources" do
    fab = Fabric.new
    fab.add("gen", "GenerateFlowFile", {"content" => "x", "batch_size" => "1"})
    with_server(fab) do |client, _|
      resp = client.get("/health")
      resp.status_code.should eq 200
      body = JSON.parse(resp.body)
      body["status"].as_s.should eq "healthy"
      body["sources"].as_a.size.should eq 1
      body["sources"][0]["name"].as_s.should eq "gen"
    end
  end

  it "GET /readyz returns 200 + ready:true when processors are present and sources running" do
    fab = Fabric.new
    fab.add("gen", "GenerateFlowFile", {"content" => "x", "batch_size" => "1"})
    with_server(fab) do |client, _|
      resp = client.get("/readyz")
      resp.status_code.should eq 200
      body = JSON.parse(resp.body)
      body["ready"].as_bool.should be_true
      body["sourcesNotRunning"].as_a.should be_empty
    end
  end

  it "GET /readyz returns 503 + ready:false when there are no processors" do
    fab = Fabric.new
    with_server(fab) do |client, _|
      resp = client.get("/readyz")
      resp.status_code.should eq 503
      JSON.parse(resp.body)["ready"].as_bool.should be_false
    end
  end

  it "GET /readyz returns 503 when a source is stopped" do
    fab = Fabric.new
    fab.add("gen", "GenerateFlowFile", {"content" => "x", "batch_size" => "1"})
    fab.toggle("gen", false)
    with_server(fab) do |client, _|
      resp = client.get("/readyz")
      resp.status_code.should eq 503
      body = JSON.parse(resp.body)
      body["sourcesNotRunning"].as_a.map(&.as_s).should eq ["gen"]
    end
  end

  it "GET /api/registry lists all registered processors" do
    with_server(Fabric.new) do |client, _|
      resp = client.get("/api/registry")
      resp.status_code.should eq 200
      names = JSON.parse(resp.body).as_a.map { |m| m["name"].as_s }
      # Spot-check a handful to confirm the registry round-trips.
      names.should contain "UpdateAttribute"
      names.should contain "ConvertJSONToRecord"
      names.should contain "PackageFlowFileV3"
    end
  end

  it "GET /metrics returns Prometheus text exposition" do
    fab = Fabric.new
    fab.add("n", "UpdateAttribute", {"key" => "k", "value" => "v"})
    with_server(fab) do |client, _|
      resp = client.get("/metrics")
      resp.status_code.should eq 200
      resp.content_type.not_nil!.should contain "text/plain"
      resp.body.should contain "zinc_processed_total"
    end
  end

  it "GET /api/flow returns processors + sources in the shared shape" do
    fab = Fabric.new
    fab.add("u", "UpdateAttribute", {"key" => "k", "value" => "v"})
    fab.add("l", "LogAttribute", {} of String => String)
    fab.connect("u", "success", "l")
    with_server(fab) do |client, _|
      body = JSON.parse(client.get("/api/flow").body)
      names = body["processors"].as_a.map { |p| p["name"].as_s }.sort
      names.should eq ["l", "u"]
    end
  end

  it "returns 404 on unknown paths (no UI root configured)" do
    with_server(Fabric.new) do |client, _|
      client.get("/nope").status_code.should eq 404
    end
  end
end
