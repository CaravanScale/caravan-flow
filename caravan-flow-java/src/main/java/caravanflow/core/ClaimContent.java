package caravanflow.core;

/// Content handle that points at a {@link ContentStore} rather than
/// holding bytes inline. Processors that need the bytes resolve the
/// claim via {@link ContentResolver#resolve(Content, ContentStore)};
/// processors that only need to move the FlowFile through the graph
/// don't need to look at the content at all.
///
/// {@code size} is recorded at claim time so stats and backpressure
/// heuristics don't need to round-trip to the store.
public record ClaimContent(String claimId, int size) implements Content {

    public ClaimContent {
        if (claimId == null || claimId.isEmpty()) {
            throw new IllegalArgumentException("ClaimContent claimId must not be blank");
        }
        if (size < 0) {
            throw new IllegalArgumentException("ClaimContent size must not be negative");
        }
    }
}
