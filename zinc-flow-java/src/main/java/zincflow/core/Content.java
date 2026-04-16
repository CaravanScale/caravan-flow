package zincflow.core;

/// FlowFile payload — a sealed hierarchy so processors can pattern-match
/// on the shape. {@link RawContent} carries bytes; {@link RecordContent}
/// carries structured records (produced by ConvertJSONToRecord and
/// similar). Claim-based content backed by a ContentStore is a
/// follow-up.
public sealed interface Content permits RawContent, RecordContent {
    /// Rough content size — bytes for RawContent, record count for
    /// RecordContent. Used for stats + backpressure heuristics.
    int size();
}
