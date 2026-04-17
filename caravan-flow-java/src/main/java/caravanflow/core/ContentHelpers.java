package caravanflow.core;

/// Lightweight decision helpers for the small-vs-large content split.
/// Small content stays inline as {@link RawContent}; anything above
/// {@link #DEFAULT_CLAIM_THRESHOLD} gets offloaded to the wired store
/// and referenced by a {@link ClaimContent}.
///
/// Mirror of caravan-flow-csharp's ContentHelpers — the threshold is
/// chosen to keep typical flowfiles (attribute-heavy JSON, small CSV
/// rows, control messages) on the heap fast path while pushing media
/// blobs and multi-MB document payloads out to disk.
public final class ContentHelpers {

    /// Default offload threshold: 256 KB. Anything bigger is worth a
    /// store round-trip; anything smaller stays inline where the CPU
    /// cost of hashing, sharding, and syscalls would outweigh heap
    /// pressure. Tunable per call by passing an explicit threshold.
    public static final int DEFAULT_CLAIM_THRESHOLD = 256 * 1024;

    private ContentHelpers() {}

    /// If {@code data} is under the threshold, wrap it in a
    /// {@link RawContent}; otherwise push it into the store and return
    /// a {@link ClaimContent}. A null store forces the inline path —
    /// callers without a store (tests, small pipelines) get the small
    /// behavior automatically.
    public static Content maybeOffload(ContentStore store, byte[] data) {
        return maybeOffload(store, data, DEFAULT_CLAIM_THRESHOLD);
    }

    public static Content maybeOffload(ContentStore store, byte[] data, int threshold) {
        if (data == null) data = new byte[0];
        if (store == null || data.length <= threshold) {
            return new RawContent(data);
        }
        String claimId = store.store(data);
        return new ClaimContent(claimId, data.length);
    }
}
