package caravanflow.fabric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/// Resolves this worker's stable identity. Returned map shape is what
/// the management API emits at {@code GET /api/identity} and what the
/// {@link caravanflow.providers.UIRegistrationProvider} sends to the UI.
///
/// <h2>Node id resolution</h2>
/// <ol>
///   <li>{@code ui.nodeId} in the effective layered config — explicit
///       operator override. Stable across restarts by construction.</li>
///   <li>UUID persisted to {@code ./caravanflow.nodeId} on first boot.
///       Survives restarts when that path lives on a volume;
///       regenerates otherwise.</li>
/// </ol>
///
/// Hostname always comes from {@link InetAddress#getLocalHost()} and
/// is kept distinct from nodeId — a k8s pod rename rotates hostname
/// but not the logical identity.
public final class NodeIdentity {

    private static final Logger log = LoggerFactory.getLogger(NodeIdentity.class);

    public static final String NODE_ID_FILE = "caravanflow.nodeId";
    public static final String CONFIG_KEY = "ui.nodeId";

    private final String nodeId;
    private final String hostname;
    private final String version;
    private final long bootMillis = System.currentTimeMillis();

    public NodeIdentity(String nodeId, String hostname, String version) {
        this.nodeId = nodeId;
        this.hostname = hostname;
        this.version = version;
    }

    public String nodeId() { return nodeId; }
    public String hostname() { return hostname; }
    public String version() { return version; }
    public long uptimeMillis() { return System.currentTimeMillis() - bootMillis; }

    /// Build an identity map suitable for JSON — the shape the UI sees
    /// at {@code GET /api/identity} and on every registration POST.
    public Map<String, Object> toMap(int port) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("nodeId", nodeId);
        out.put("hostname", hostname);
        out.put("version", version);
        out.put("port", port);
        out.put("uptimeMillis", uptimeMillis());
        out.put("bootMillis", bootMillis);
        return out;
    }

    /// Resolve identity at startup.
    public static NodeIdentity resolve(Map<String, Object> effectiveConfig,
                                       Path nodeIdFile,
                                       String version) {
        String configured = readConfigNodeId(effectiveConfig);
        String nodeId = configured != null ? configured : loadOrCreatePersistedNodeId(nodeIdFile);
        String host = resolveHostname();
        return new NodeIdentity(nodeId, host, version == null ? "unknown" : version);
    }

    @SuppressWarnings("unchecked")
    private static String readConfigNodeId(Map<String, Object> effectiveConfig) {
        if (effectiveConfig == null) return null;
        Object ui = effectiveConfig.get("ui");
        if (!(ui instanceof Map<?, ?> uiMap)) return null;
        Object v = ((Map<String, Object>) uiMap).get("nodeId");
        return v == null ? null : v.toString();
    }

    private static String loadOrCreatePersistedNodeId(Path path) {
        if (path == null) path = Path.of(NODE_ID_FILE);
        try {
            if (Files.exists(path)) {
                String existing = Files.readString(path).trim();
                if (!existing.isEmpty()) return existing;
            }
            String fresh = UUID.randomUUID().toString();
            Files.writeString(path, fresh);
            log.info("generated fresh node id {} → {}", fresh, path);
            return fresh;
        } catch (IOException ex) {
            // Disk I/O failure shouldn't take the worker down. Fall back
            // to an ephemeral UUID — the operator can set ui.nodeId in
            // config to fix it properly.
            log.warn("node id persistence failed for {} ({}) — using ephemeral id", path, ex.toString());
            return UUID.randomUUID().toString();
        }
    }

    private static String resolveHostname() {
        try { return InetAddress.getLocalHost().getHostName(); }
        catch (UnknownHostException ex) {
            String env = System.getenv("HOSTNAME");
            return env == null ? "unknown" : env;
        }
    }
}
