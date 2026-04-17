package caravanflow.ui.views;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import caravanflow.shared.NodeEntry;
import caravanflow.ui.NodeRegistry;

import java.util.Map;

/// UI ingress endpoints for worker self-registration. Phase 1 only
/// parks entries in memory; Phase 2 surfaces them in the /nodes
/// view. Accepts the worker's identity payload — the same shape
/// {@code NodeIdentity.toMap} emits — and extracts nodeId +
/// hostname + version + port.
///
/// The worker's {@code UIRegistrationProvider} posts the same JSON
/// body to both paths, so /register and /heartbeat route through one
/// handler; the only observable difference is the {@code source}
/// field on the resulting entry ("self" in both cases — static
/// seeding will land when multi-worker config arrives in Phase 2).
public final class RegistryController {

    private static final Logger log = LoggerFactory.getLogger(RegistryController.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final NodeRegistry registry;

    public RegistryController(NodeRegistry registry) {
        this.registry = registry;
    }

    public void handleRegister(Context ctx) throws Exception {
        NodeEntry entry = ingest(ctx);
        if (entry == null) return;
        log.info("registered node {} ({}) from {}", entry.nodeId(), entry.hostname(), entry.url());
        ctx.status(201).contentType("application/json").result(JSON.writeValueAsBytes(Map.of(
                "status", "registered",
                "nodeId", entry.nodeId())));
    }

    public void handleHeartbeat(Context ctx) throws Exception {
        NodeEntry entry = ingest(ctx);
        if (entry == null) return;
        log.debug("heartbeat from {} ({})", entry.nodeId(), entry.hostname());
        ctx.status(200).contentType("application/json").result(JSON.writeValueAsBytes(Map.of(
                "status", "ok",
                "nodeId", entry.nodeId())));
    }

    public void handleList(Context ctx) throws Exception {
        ctx.contentType("application/json").result(JSON.writeValueAsBytes(registry.list()));
    }

    /// Parse and record the POST body. Writes a 400 + returns null on
    /// bad input — callers should short-circuit when null comes back.
    @SuppressWarnings("unchecked")
    private NodeEntry ingest(Context ctx) throws Exception {
        byte[] body = ctx.bodyAsBytes();
        if (body == null || body.length == 0) {
            ctx.status(400).result("empty body");
            return null;
        }
        Map<String, Object> payload;
        try {
            payload = JSON.readValue(body, Map.class);
        } catch (Exception ex) {
            ctx.status(400).result("invalid json: " + ex.getMessage());
            return null;
        }
        String nodeId = str(payload.get("nodeId"));
        if (nodeId == null || nodeId.isBlank()) {
            ctx.status(400).result("payload missing nodeId");
            return null;
        }
        String hostname = str(payload.get("hostname"));
        String version  = str(payload.get("version"));
        String url      = workerUrlFromPayload(ctx, payload);
        return registry.record(nodeId, url, hostname, version, "self");
    }

    /// The worker's identity map doesn't carry its own full URL —
    /// just {@code port}. We reconstruct {@code http://<host>:<port>}
    /// where host is the client-visible remote address (what the UI
    /// saw when the request hit). Falls back to hostname from the
    /// payload if the remote address isn't resolvable.
    private static String workerUrlFromPayload(Context ctx, Map<String, Object> payload) {
        Object port = payload.get("port");
        String host = ctx.req().getRemoteAddr();
        if (host == null || host.isBlank()) host = str(payload.get("hostname"));
        if (host == null || host.isBlank()) return null;
        return "http://" + host + (port == null ? "" : (":" + port));
    }

    private static String str(Object o) { return o == null ? null : o.toString(); }
}
