package zincflow.fabric;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.FlowFile;

import java.util.HashMap;
import java.util.Map;

/// Minimal HTTP surface — one ingest endpoint + one stats endpoint.
/// Full API parity (processor list, connections, flow graph, providers,
/// provenance, metrics, enable/disable, hot reload) lands in Phase 3.
///
/// Uses Javalin 6 (Jetty 12 underneath) — lightweight, non-DI, minimal
/// configuration surface.
public final class HttpServer {

    private static final Logger log = LoggerFactory.getLogger(HttpServer.class);

    private final Pipeline pipeline;
    private final ObjectMapper json = new ObjectMapper();
    private Javalin app;
    private int boundPort = -1;

    public HttpServer(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    /// Starts the server on {@code port}. Pass {@code 0} to let the OS
    /// pick a free port (handy in tests). Returns {@code this} so calls
    /// can chain.
    public HttpServer start(int port) {
        app = Javalin.create(cfg -> { /* default config — no DI, no plugins */ })
                .post("/", this::handleIngest)
                .get("/api/stats", this::handleStats)
                .get("/health", ctx -> ctx.result("ok"));
        app.start(port);
        boundPort = app.port();
        log.info("zinc-flow HTTP server listening on http://localhost:{}", boundPort);
        return this;
    }

    public int port() {
        if (boundPort < 0) {
            throw new IllegalStateException("server not started");
        }
        return boundPort;
    }

    public void stop() {
        if (app != null) {
            app.stop();
            app = null;
            boundPort = -1;
        }
    }

    private void handleIngest(Context ctx) {
        byte[] body = ctx.bodyAsBytes();
        Map<String, String> attributes = new HashMap<>();
        // Map every X-Flow-* header (except X-Flow-Type which is normalised to "type") into attributes.
        ctx.headerMap().forEach((k, v) -> {
            if (k == null) return;
            String lower = k.toLowerCase();
            if (lower.startsWith("x-flow-")) {
                String attrKey = lower.substring("x-flow-".length());
                attributes.put(attrKey, v);
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
}
