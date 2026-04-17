package caravanflow.shared;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/// One provenance event as emitted by {@code GET /api/provenance*}.
/// Matches {@code caravanflow.providers.ProvenanceProvider.Event} on the
/// worker. {@code type} is serialized as its enum name
/// (CREATED, PROCESSED, ROUTED, DROPPED, FAILED).
@JsonIgnoreProperties(ignoreUnknown = true)
public record ProvenanceEvent(
        long flowFileId,
        String type,
        String component,
        String details,
        long timestampMillis) {
}
