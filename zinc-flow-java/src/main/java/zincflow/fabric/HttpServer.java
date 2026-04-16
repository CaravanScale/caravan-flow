package zincflow.fabric;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorContext;
import zincflow.core.Provider;
import zincflow.core.Source;
import zincflow.providers.ProvenanceProvider;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// HTTP surface for zinc-flow-java. Phase 3 adds:
///   * POST /             — ingest a FlowFile
///   * GET  /health       — liveness probe
///   * GET  /api/stats    — running counters
///   * GET  /api/processors — list active processors + their type
///   * GET  /api/connections — full connection map
///   * GET  /api/flow     — full graph snapshot (processors + connections + stats)
///   * POST /api/reload   — hot-swap the graph from config.yaml on disk
///
/// Javalin 6 with Jetty 12 underneath — non-DI, minimal surface.
public final class HttpServer {

    private static final Logger log = LoggerFactory.getLogger(HttpServer.class);

    private final Pipeline pipeline;
    private final ConfigLoader loader;
    private final Path configPath;
    private final PluginLoader.Summary plugins;
    private final Path pluginsDir;
    private final ObjectMapper json = new ObjectMapper();
    private Javalin app;
    private int boundPort = -1;

    public HttpServer(Pipeline pipeline) {
        this(pipeline, null, null, null, null);
    }

    /// Constructor for the config-driven path — enables {@code /api/reload}.
    public HttpServer(Pipeline pipeline, ConfigLoader loader, Path configPath) {
        this(pipeline, loader, configPath, null, null);
    }

    /// Full constructor — wires in the plugin summary so {@code /api/plugins}
    /// can report what was discovered at startup.
    public HttpServer(Pipeline pipeline, ConfigLoader loader, Path configPath,
                      PluginLoader.Summary plugins, Path pluginsDir) {
        this.pipeline = pipeline;
        this.loader = loader;
        this.configPath = configPath;
        this.plugins = plugins == null ? PluginLoader.Summary.empty() : plugins;
        this.pluginsDir = pluginsDir;
    }

    public HttpServer start(int port) {
        app = Javalin.create(cfg -> { /* default config — no DI, no plugins */ })
                // GET / serves the dashboard when the static file is on the
                // classpath. POST / is ingest. Distinguishing by method is a
                // zincflow-isms since the original C# service followed the
                // same convention.
                .get("/",                              this::handleDashboard)
                .post("/",                             this::handleIngest)
                .get("/dashboard",                     this::handleDashboard)
                .get("/health",                        this::handleHealth)
                .get("/metrics",                       this::handleMetrics)
                .get("/api/stats",                     this::handleStats)
                .get("/api/processors",                this::handleProcessors)
                .get("/api/processor-stats",           this::handleProcessorStats)
                .get("/api/connections",               this::handleConnections)
                .get("/api/flow",                      this::handleFlow)
                .get("/api/registry",                  this::handleRegistry)
                .post("/api/reload",                   this::handleReload)
                .get("/api/providers",                 this::handleProviders)
                .post("/api/providers/enable",         this::handleEnableProvider)
                .post("/api/providers/disable",        this::handleDisableProvider)
                .get("/api/provenance",                this::handleProvenanceRecent)
                .get("/api/provenance/{id}",           this::handleProvenanceById)
                .post("/api/processors/add",           this::handleAddProcessor)
                .delete("/api/processors/remove",      this::handleRemoveProcessor)
                .post("/api/processors/enable",        this::handleEnableProcessor)
                .post("/api/processors/disable",       this::handleDisableProcessor)
                .post("/api/processors/state",         this::handleProcessorState)
                .get("/api/sources",                   this::handleSources)
                .post("/api/sources/start",            this::handleStartSource)
                .post("/api/sources/stop",             this::handleStopSource)
                .get("/api/plugins",                   this::handlePlugins)
                .post("/api/plugins/reload",           this::handleReloadPlugins);
        app.start(port);
        boundPort = app.port();
        log.info("zinc-flow HTTP server listening on http://localhost:{}", boundPort);
        return this;
    }

    public int port() {
        if (boundPort < 0) throw new IllegalStateException("server not started");
        return boundPort;
    }

    public void stop() {
        if (app != null) {
            app.stop();
            app = null;
            boundPort = -1;
        }
    }

    // --- Handlers ---

    private void handleIngest(Context ctx) {
        byte[] body = ctx.bodyAsBytes();
        Map<String, String> attributes = new HashMap<>();
        ctx.headerMap().forEach((k, v) -> {
            if (k == null) return;
            String lower = k.toLowerCase();
            if (lower.startsWith("x-flow-")) {
                attributes.put(lower.substring("x-flow-".length()), v);
            }
        });
        FlowFile ff = FlowFile.create(body, attributes);
        try {
            pipeline.ingest(ff);
            ctx.status(202).result(ff.stringId());
        } catch (RuntimeException ex) {
            log.error("ingest failed for {}: {}", ff.stringId(), ex.toString(), ex);
            ctx.status(500).result("pipeline error: " + ex.getMessage());
        }
    }

    private void handleStats(Context ctx) throws Exception {
        ctx.contentType("application/json").result(json.writeValueAsBytes(pipeline.stats().snapshot()));
    }

    private void handleProcessors(Context ctx) throws Exception {
        PipelineGraph graph = pipeline.graph();
        Map<String, String> out = new LinkedHashMap<>();
        for (var entry : graph.processors().entrySet()) {
            out.put(entry.getKey(), pipeline.processorType(entry.getKey()));
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(out));
    }

    private void handleProcessorStats(Context ctx) throws Exception {
        ctx.contentType("application/json").result(json.writeValueAsBytes(pipeline.processorStats()));
    }

    private void handleConnections(Context ctx) throws Exception {
        ctx.contentType("application/json").result(json.writeValueAsBytes(pipeline.graph().connections()));
    }

    private void handleFlow(Context ctx) throws Exception {
        PipelineGraph graph = pipeline.graph();
        List<Map<String, Object>> processors = new ArrayList<>();
        Map<String, Map<String, Long>> perProcStats = pipeline.processorStats();
        for (var entry : graph.processors().entrySet()) {
            String name = entry.getKey();
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("name", name);
            info.put("type", pipeline.processorType(name));
            info.put("state", pipeline.processorState(name).name());
            info.put("stats", perProcStats.getOrDefault(name, Map.of()));
            info.put("connections", graph.connections().getOrDefault(name, Map.of()));
            processors.add(info);
        }

        List<Map<String, Object>> providers = new ArrayList<>();
        ProcessorContext pctx = pipeline.context();
        for (String pname : pctx.listProviders()) {
            Provider p = pctx.getProvider(pname);
            providers.add(Map.of(
                    "name",  pname,
                    "type",  p == null ? "unknown" : p.providerType(),
                    "state", p == null ? "UNKNOWN" : p.state().name()));
        }

        List<Map<String, Object>> srcs = new ArrayList<>();
        for (Source s : pipeline.sources().values()) {
            srcs.add(Map.of(
                    "name", s.name(),
                    "type", s.sourceType(),
                    "running", s.isRunning()));
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("entryPoints", graph.entryPoints());
        out.put("processors",  processors);
        out.put("connections", graph.connections());
        out.put("providers",   providers);
        out.put("sources",     srcs);
        out.put("stats",       pipeline.stats().snapshot());
        ctx.contentType("application/json").result(json.writeValueAsBytes(out));
    }

    private void handleRegistry(Context ctx) throws Exception {
        List<String> types;
        if (pipeline.registry() == null) {
            types = List.of();
        } else {
            types = new ArrayList<>(pipeline.registry().types());
            java.util.Collections.sort(types);
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(types));
    }

    private void handleMetrics(Context ctx) {
        ctx.contentType("text/plain; version=0.0.4; charset=utf-8")
           .result(pipeline.metrics().scrape());
    }

    private void handleDashboard(Context ctx) {
        try (var in = HttpServer.class.getClassLoader().getResourceAsStream("dashboard.html")) {
            if (in == null) {
                ctx.status(404).result("dashboard.html not on classpath");
                return;
            }
            ctx.contentType("text/html; charset=utf-8").result(in.readAllBytes());
        } catch (java.io.IOException ex) {
            ctx.status(500).result("dashboard read failed: " + ex.getMessage());
        }
    }

    private void handleReload(Context ctx) {
        if (loader == null || configPath == null) {
            ctx.status(501).result("reload not supported: server started without a config loader");
            return;
        }
        try {
            PipelineGraph fresh = loader.loadFromFile(configPath);
            pipeline.swapGraph(fresh);
            log.info("reloaded pipeline from {} — {} processors, entry points: {}",
                    configPath, fresh.processors().size(), fresh.entryPoints());
            ctx.status(200).result("reloaded");
        } catch (IOException | RuntimeException ex) {
            log.error("reload failed: {}", ex.toString(), ex);
            ctx.status(400).result("reload failed: " + ex.getMessage());
        }
    }

    // --- Health ---

    private void handleHealth(Context ctx) throws Exception {
        List<Map<String, Object>> srcs = new ArrayList<>();
        for (Source s : pipeline.sources().values()) {
            srcs.add(Map.of("name", s.name(), "type", s.sourceType(), "running", s.isRunning()));
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(Map.of(
                "status", "healthy",
                "sources", srcs)));
    }

    // --- Providers ---

    private void handleProviders(Context ctx) throws Exception {
        List<Map<String, Object>> out = new ArrayList<>();
        ProcessorContext pctx = pipeline.context();
        for (String name : pctx.listProviders()) {
            Provider p = pctx.getProvider(name);
            out.add(Map.of(
                    "name",  name,
                    "type",  p == null ? "unknown" : p.providerType(),
                    "state", p == null ? "UNKNOWN" : p.state().name()));
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(out));
    }

    private void handleEnableProvider(Context ctx) throws Exception {
        String name = nameFromBody(ctx);
        if (name.isEmpty()) { writeError(ctx, 400, "name required"); return; }
        boolean ok = pipeline.enableProvider(name);
        writeStatus(ctx, ok, name, "enabled", "provider not found");
    }

    private void handleDisableProvider(Context ctx) throws Exception {
        String name = nameFromBody(ctx);
        if (name.isEmpty()) { writeError(ctx, 400, "name required"); return; }
        boolean ok = pipeline.disableProvider(name);
        writeStatus(ctx, ok, name, "disabled", "provider not found");
    }

    // --- Provenance ---

    private void handleProvenanceRecent(Context ctx) throws Exception {
        ProvenanceProvider prov = pipeline.provenance();
        if (prov == null) { writeError(ctx, 503, "provenance provider not enabled"); return; }
        int n = 50;
        String nStr = ctx.queryParam("n");
        if (nStr != null && !nStr.isEmpty()) {
            try { n = Integer.parseInt(nStr); }
            catch (NumberFormatException e) { writeError(ctx, 400, "query param 'n' is not an integer: '" + nStr + "'"); return; }
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(shape(prov.getRecent(n))));
    }

    private void handleProvenanceById(Context ctx) throws Exception {
        ProvenanceProvider prov = pipeline.provenance();
        if (prov == null) { writeError(ctx, 503, "provenance provider not enabled"); return; }
        long id;
        try { id = Long.parseLong(ctx.pathParam("id")); }
        catch (NumberFormatException e) { writeError(ctx, 400, "path param 'id' is not a long"); return; }
        ctx.contentType("application/json").result(json.writeValueAsBytes(shape(prov.getEvents(id))));
    }

    private static List<Map<String, Object>> shape(List<ProvenanceProvider.Event> events) {
        List<Map<String, Object>> out = new ArrayList<>(events.size());
        for (ProvenanceProvider.Event e : events) {
            out.add(Map.of(
                    "flowfile",  "ff-" + e.flowFileId(),
                    "type",      e.type().name(),
                    "component", e.component(),
                    "details",   e.details(),
                    "timestamp", e.timestampMillis()));
        }
        return out;
    }

    // --- Processor admin ---

    private void handleAddProcessor(Context ctx) throws Exception {
        Map<String, Object> body = readJsonBody(ctx);
        if (body == null) { writeError(ctx, 400, "invalid json body"); return; }
        String name = str(body.get("name"));
        String type = str(body.get("type"));
        if (name.isEmpty() || type.isEmpty()) { writeError(ctx, 400, "name and type required"); return; }

        Map<String, String> config = asStringMap(body.get("config"));
        List<String> requires = asStringList(body.get("requires"));
        Map<String, List<String>> connections = asConnections(body.get("connections"));

        boolean ok = pipeline.addProcessor(name, type, config, requires, connections);
        writeStatus(ctx, ok, name, "created", "processor already exists or unknown type");
    }

    private void handleRemoveProcessor(Context ctx) throws Exception {
        String name = nameFromBody(ctx);
        if (name.isEmpty()) { writeError(ctx, 400, "name required"); return; }
        boolean ok = pipeline.removeProcessor(name);
        writeStatus(ctx, ok, name, "removed", "processor not found");
    }

    private void handleEnableProcessor(Context ctx) throws Exception {
        String name = nameFromBody(ctx);
        if (name.isEmpty()) { writeError(ctx, 400, "name required"); return; }
        boolean ok = pipeline.enableProcessor(name);
        writeStatus(ctx, ok, name, "enabled", "processor not found");
    }

    private void handleDisableProcessor(Context ctx) throws Exception {
        String name = nameFromBody(ctx);
        if (name.isEmpty()) { writeError(ctx, 400, "name required"); return; }
        boolean ok = pipeline.disableProcessor(name);
        writeStatus(ctx, ok, name, "disabled", "processor not found");
    }

    private void handleProcessorState(Context ctx) throws Exception {
        String name = nameFromBody(ctx);
        if (name.isEmpty()) { writeError(ctx, 400, "name required"); return; }
        if (!pipeline.graph().processors().containsKey(name)) { writeError(ctx, 404, "processor not found"); return; }
        ctx.contentType("application/json").result(json.writeValueAsBytes(Map.of(
                "name",  name,
                "state", pipeline.processorState(name).name())));
    }

    // --- Sources ---

    private void handleSources(Context ctx) throws Exception {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Source s : pipeline.sources().values()) {
            out.add(Map.of("name", s.name(), "type", s.sourceType(), "running", s.isRunning()));
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(out));
    }

    private void handleStartSource(Context ctx) throws Exception {
        String name = nameFromBody(ctx);
        if (name.isEmpty()) { writeError(ctx, 400, "name required"); return; }
        boolean ok = pipeline.startSource(name);
        writeStatus(ctx, ok, name, "started", "source not found");
    }

    private void handleStopSource(Context ctx) throws Exception {
        String name = nameFromBody(ctx);
        if (name.isEmpty()) { writeError(ctx, 400, "name required"); return; }
        boolean ok = pipeline.stopSource(name);
        writeStatus(ctx, ok, name, "stopped", "source not found");
    }

    // --- Plugins ---

    private PluginLoader.Summary currentPlugins = null;

    private PluginLoader.Summary pluginSummary() {
        return currentPlugins != null ? currentPlugins : plugins;
    }

    private void handlePlugins(Context ctx) throws Exception {
        ctx.contentType("application/json")
           .result(json.writeValueAsBytes(PluginLoader.toJson(pluginSummary())));
    }

    /// Re-scan the plugins directory and register any new jars that
    /// appeared since startup. Existing entries are overwritten (last
    /// loader wins) — convenient for dev, relies on the operator to
    /// avoid removing a processor type that's still referenced in the
    /// running flow.
    private void handleReloadPlugins(Context ctx) throws Exception {
        if (pluginsDir == null) {
            writeError(ctx, 501, "plugins directory was not configured at startup");
            return;
        }
        if (pipeline.registry() == null) {
            writeError(ctx, 501, "pipeline has no registry wired — plugin reload unavailable");
            return;
        }
        currentPlugins = PluginLoader.loadFromDirectory(pluginsDir, pipeline.registry(), pipeline.context());
        log.info("reloaded plugins from {} — {} loaded", pluginsDir, currentPlugins.totalLoaded());
        ctx.contentType("application/json")
           .result(json.writeValueAsBytes(PluginLoader.toJson(currentPlugins)));
    }

    // --- Body + response helpers ---

    @SuppressWarnings("unchecked")
    private Map<String, Object> readJsonBody(Context ctx) {
        try { return json.readValue(ctx.bodyAsBytes(), Map.class); }
        catch (Exception e) { return null; }
    }

    private String nameFromBody(Context ctx) {
        Map<String, Object> body = readJsonBody(ctx);
        return body == null ? "" : str(body.get("name"));
    }

    private static String str(Object o) { return o == null ? "" : o.toString(); }

    @SuppressWarnings("unchecked")
    private static Map<String, String> asStringMap(Object raw) {
        if (!(raw instanceof Map<?, ?> m)) return Map.of();
        Map<String, String> out = new LinkedHashMap<>();
        for (var entry : m.entrySet()) out.put(String.valueOf(entry.getKey()), str(entry.getValue()));
        return out;
    }

    @SuppressWarnings("unchecked")
    private static List<String> asStringList(Object raw) {
        if (!(raw instanceof List<?> l)) return List.of();
        List<String> out = new ArrayList<>(l.size());
        for (Object o : l) out.add(str(o));
        return out;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<String>> asConnections(Object raw) {
        if (!(raw instanceof Map<?, ?> m)) return Map.of();
        Map<String, List<String>> out = new LinkedHashMap<>();
        for (var entry : m.entrySet()) {
            String rel = String.valueOf(entry.getKey());
            if (entry.getValue() instanceof List<?> l) {
                List<String> targets = new ArrayList<>(l.size());
                for (Object o : l) targets.add(str(o));
                out.put(rel, targets);
            } else if (entry.getValue() != null) {
                out.put(rel, List.of(str(entry.getValue())));
            }
        }
        return out;
    }

    private void writeStatus(Context ctx, boolean ok, String name, String okStatus, String failMessage) throws Exception {
        Map<String, Object> body = ok
                ? Map.of("status", okStatus, "name", name)
                : Map.of("error", failMessage, "name", name);
        ctx.status(ok ? 200 : 404)
           .contentType("application/json")
           .result(json.writeValueAsBytes(body));
    }

    private void writeError(Context ctx, int status, String message) throws Exception {
        ctx.status(status)
           .contentType("application/json")
           .result(json.writeValueAsBytes(Map.of("error", message)));
    }

    // Exposed for tests/inspection.
    public List<String> routes() {
        return List.of(
                "GET /", "POST /", "GET /dashboard", "GET /health", "GET /metrics",
                "GET /api/stats", "GET /api/processors", "GET /api/processor-stats",
                "GET /api/connections", "GET /api/flow", "GET /api/registry",
                "POST /api/reload",
                "GET /api/providers", "POST /api/providers/enable", "POST /api/providers/disable",
                "GET /api/provenance", "GET /api/provenance/{id}",
                "POST /api/processors/add", "DELETE /api/processors/remove",
                "POST /api/processors/enable", "POST /api/processors/disable",
                "POST /api/processors/state",
                "GET /api/sources", "POST /api/sources/start", "POST /api/sources/stop",
                "GET /api/plugins", "POST /api/plugins/reload");
    }
}
