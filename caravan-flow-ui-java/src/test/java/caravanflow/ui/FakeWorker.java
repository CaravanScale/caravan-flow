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
                        "stats", Map.of("processed", 3L),
                        "connections", Map.of("success", java.util.List.of("tail"))),
                Map.of("name", "tail", "type", "LogAttribute", "state", "ENABLED",
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
}
