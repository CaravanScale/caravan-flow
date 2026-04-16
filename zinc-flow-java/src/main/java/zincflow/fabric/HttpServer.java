package zincflow.fabric;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.FlowFile;
import zincflow.core.Processor;

import java.io.IOException;
import java.nio.file.Path;
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
    private final ObjectMapper json = new ObjectMapper();
    private Javalin app;
    private int boundPort = -1;

    public HttpServer(Pipeline pipeline) {
        this(pipeline, null, null);
    }

    /// Constructor for the config-driven path — enables {@code /api/reload}.
    public HttpServer(Pipeline pipeline, ConfigLoader loader, Path configPath) {
        this.pipeline = pipeline;
        this.loader = loader;
        this.configPath = configPath;
    }

    public HttpServer start(int port) {
        app = Javalin.create(cfg -> { /* default config — no DI, no plugins */ })
                .post("/",                 this::handleIngest)
                .get("/health",            ctx -> ctx.result("ok"))
                .get("/api/stats",         this::handleStats)
                .get("/api/processors",    this::handleProcessors)
                .get("/api/connections",   this::handleConnections)
                .get("/api/flow",          this::handleFlow)
                .post("/api/reload",       this::handleReload);
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
            Processor p = entry.getValue();
            out.put(entry.getKey(), p.getClass().getSimpleName());
        }
        ctx.contentType("application/json").result(json.writeValueAsBytes(out));
    }

    private void handleConnections(Context ctx) throws Exception {
        ctx.contentType("application/json").result(json.writeValueAsBytes(pipeline.graph().connections()));
    }

    private void handleFlow(Context ctx) throws Exception {
        PipelineGraph graph = pipeline.graph();
        Map<String, String> procTypes = new LinkedHashMap<>();
        for (var entry : graph.processors().entrySet()) {
            procTypes.put(entry.getKey(), entry.getValue().getClass().getSimpleName());
        }
        Map<String, Object> out = Map.of(
                "entryPoints", graph.entryPoints(),
                "processors",  procTypes,
                "connections", graph.connections(),
                "stats",       pipeline.stats().snapshot());
        ctx.contentType("application/json").result(json.writeValueAsBytes(out));
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

    // Exposed for tests/inspection.
    public List<String> routes() {
        return List.of("POST /", "GET /health", "GET /api/stats", "GET /api/processors",
                "GET /api/connections", "GET /api/flow", "POST /api/reload");
    }
}
