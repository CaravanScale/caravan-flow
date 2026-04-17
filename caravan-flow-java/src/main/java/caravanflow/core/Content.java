package caravanflow.core;

/// FlowFile payload — a sealed hierarchy so processors can pattern-match
/// on the shape. {@link RawContent} carries bytes inline;
/// {@link RecordContent} carries structured records (produced by
/// ConvertJSONToRecord and similar); {@link ClaimContent} references
/// bytes stored in a {@link ContentStore}.
public sealed interface Content permits RawContent, RecordContent, ClaimContent {
    /// Rough content size — bytes for RawContent, record count for
    /// RecordContent. Used for stats + backpressure heuristics.
    int size();
}
