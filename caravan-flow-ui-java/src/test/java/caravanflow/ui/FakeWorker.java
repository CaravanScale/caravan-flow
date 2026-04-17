package caravanflow.ui;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/// Tiny test double that impersonates a caravan-flow worker's
/// management API. Each handler is opt-in — register the routes the
/// test needs and leave the rest to 404 so missing expectations
/// surface loudly.
final class FakeWorker {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Javalin app = Javalin.create();
    private final AtomicInteger identityHits = new AtomicInteger();
    private int boundPort = -1;

    FakeWorker withIdentity(Map<String, Object> body) {
        app.get("/api/identity", ctx -> {
            identityHits.incrementAndGet();
            ctx.contentType("application/json").result(JSON.writeValueAsBytes(body));
        });
        return this;
    }

    FakeWorker withFlow(Map<String, Object> body) {
        app.get("/api/flow", ctx ->
                ctx.contentType("application/json").result(JSON.writeValueAsBytes(body)));
        return this;
    }

    FakeWorker withFailures(java.util.List<Map<String, Object>> body) {
        app.get("/api/provenance/failures", ctx ->
                ctx.contentType("application/json").result(JSON.writeValueAsBytes(body)));
        return this;
    }

    FakeWorker withLineage(long id, java.util.List<Map<String, Object>> body) {
        app.get("/api/provenance/" + id, ctx ->
                ctx.contentType("application/json").result(JSON.writeValueAsBytes(body)));
        return this;
    }

    FakeWorker withOverlays(Map<String, Object> body) {
        app.get("/api/overlays", ctx ->
                ctx.contentType("application/json").result(JSON.writeValueAsBytes(body)));
        return this;
    }

    static Map<String, Object> sampleOverlays() {
        Map<String, Object> base = new LinkedHashMap<>();
        base.put("base", "/etc/caravan/config.yaml");
        base.put("layers", java.util.List.of(
                Map.of("role", "base",    "path", "/etc/caravan/config.yaml",    "present", true,  "size", 3),
                Map.of("role", "local",   "path", "/etc/caravan/config.local.yaml","present", true,  "size", 1),
                Map.of("role", "secrets", "path", "/etc/caravan/secrets.yaml",   "present", false, "size", 0)));
        base.put("effective", Map.of("flow", Map.of("entryPoints", java.util.List.of("ingress")),
                                     "http", Map.of("port", 9092)));
        base.put("provenance", Map.of(
                "flow.entryPoints", "base",
                "http.port",        "local"));
        return base;
    }

    static Map<String, Object> event(long flowFileId, String type, String component, String details, long ts) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("flowFileId", flowFileId);
        e.put("type", type);
        e.put("component", component);
        e.put("details", details);
        e.put("timestampMillis", ts);
        return e;
    }

    FakeWorker start() {
        app.start(0);
        boundPort = app.port();
        return this;
    }

    URI url() { return URI.create("http://localhost:" + boundPort); }

    int identityHits() { return identityHits.get(); }

    void stop() { app.stop(); }

    static Map<String, Object> sampleIdentity() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("nodeId", "node-test");
        out.put("hostname", "unit-test");
        out.put("version", "9.9.9");
        out.put("port", 9092);
        out.put("uptimeMillis", 123L);
        out.put("bootMillis", 1000L);
        return out;
    }

    static Map<String, Object> sampleFlow() {
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("entryPoints", java.util.List.of("ingress"));
        flow.put("processors", java.util.List.of(
                Map.of("name", "ingress", "type", "LogAttribute", "state", "ENABLED",
                        "config", Map.of(),
                        "stats", Map.of("processed", 3L),
                        "connections", Map.of("success", java.util.List.of("tail"))),
                Map.of("name", "tail", "type", "LogAttribute", "state", "ENABLED",
                        "config", Map.of(),
                        "stats", Map.of("processed", 3L),
                        "connections", Map.of())));
        flow.put("connections", Map.of(
                "ingress", Map.of("success", java.util.List.of("tail"))));
        flow.put("providers", java.util.List.of(
                Map.of("name", "logging", "type", "LoggingProvider", "state", "ENABLED")));
        flow.put("sources", java.util.List.of());
        flow.put("stats", Map.of("processed", 3L));
        return flow;
    }

    /// Like {@link #sampleFlow()} but with non-empty processor config
    /// — exercises the drawer's config-table render path.
    static Map<String, Object> sampleFlowWithConfig() {
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("entryPoints", java.util.List.of("ingress"));
        flow.put("processors", java.util.List.of(
                Map.of("name", "ingress", "type", "LogAttribute", "state", "ENABLED",
                        "config", Map.of("prefix", "[in] "),
                        "stats", Map.of("processed", 0L),
                        "connections", Map.of())));
        flow.put("connections", Map.of());
        flow.put("providers", java.util.List.of());
        flow.put("sources", java.util.List.of());
        flow.put("stats", Map.of());
        return flow;
    }
}
