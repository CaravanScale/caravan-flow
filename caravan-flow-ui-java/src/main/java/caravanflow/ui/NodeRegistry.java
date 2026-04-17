package caravanflow.ui;

import caravanflow.shared.NodeEntry;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/// Registry of known workers. Entries arrive one of two ways: a
/// static list seeded at startup ({@code source="static"}) or worker
/// self-registration via {@code POST /api/registry/register}
/// ({@code source="self"}). Phase 1 accepts both but only exposes
/// the single-worker identity view — full multi-node aggregation
/// ships in Phase 2.
///
/// Thread-safe: ingress can hit any node's entry while the /nodes
/// view reads the snapshot concurrently.
public final class NodeRegistry {

    private final ConcurrentMap<String, NodeEntry> byNodeId = new ConcurrentHashMap<>();

    /// Register or refresh an entry. Called by the register and
    /// heartbeat handlers with a payload the worker's
    /// {@code UIRegistrationProvider} posts — which is the worker's
    /// identity map. The method is idempotent: same nodeId → merge,
    /// always bumping {@code lastHeartbeatMillis} to now.
    public NodeEntry record(String nodeId, String url, String hostname, String version, String source) {
        if (nodeId == null || nodeId.isBlank())
            throw new IllegalArgumentException("nodeId must be non-blank");
        NodeEntry entry = new NodeEntry(
                nodeId,
                url,
                hostname,
                version,
                System.currentTimeMillis(),
                source);
        byNodeId.put(nodeId, entry);
        return entry;
    }

    public NodeEntry get(String nodeId) {
        return byNodeId.get(nodeId);
    }

    public List<NodeEntry> list() {
        Collection<NodeEntry> snapshot = byNodeId.values();
        return List.copyOf(snapshot);
    }

    public int size() { return byNodeId.size(); }

    public void clear() { byNodeId.clear(); }
}
