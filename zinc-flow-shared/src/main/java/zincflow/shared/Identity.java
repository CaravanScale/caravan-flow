package zincflow.shared;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/// Worker identity, as emitted by {@code GET /api/identity}. Fields
/// must stay in sync with {@code zincflow.fabric.NodeIdentity.toMap}
/// on the worker side — this DTO is the source of truth for the wire
/// shape from the UI's perspective.
///
/// {@code @JsonIgnoreProperties(ignoreUnknown = true)} so a worker
/// rev that adds fields doesn't break the UI.
@JsonIgnoreProperties(ignoreUnknown = true)
public record Identity(
        String nodeId,
        String hostname,
        String version,
        int port,
        long uptimeMillis,
        long bootMillis) {
}
