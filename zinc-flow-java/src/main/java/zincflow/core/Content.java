package zincflow.core;

/// FlowFile payload — a sealed hierarchy so processors can pattern-match
/// on the shape. For Phase 2 we ship {@link RawContent}; record-oriented
/// and claim-based variants come in Phase 3.
public sealed interface Content permits RawContent {
    /// Rough content size in bytes (or records, depending on variant).
    /// Used for stats + backpressure.
    int size();
}
