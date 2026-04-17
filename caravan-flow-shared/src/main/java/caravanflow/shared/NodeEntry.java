package caravanflow.shared;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/// One entry in the UI's node registry. Populated either from the
/// static list at startup or from worker self-registration
/// ({@code POST /api/registry/register}). {@code source} is "static"
/// or "self".
///
/// Phase 1 uses this in {@code NodeRegistry} even though the /nodes
/// view is a placeholder — keeping the DTO locked prevents a later
/// wire break when Phase 2 surfaces it.
@JsonIgnoreProperties(ignoreUnknown = true)
public record NodeEntry(
        String nodeId,
        String url,
        String hostname,
        String version,
        long lastHeartbeatMillis,
        String source) {
}
