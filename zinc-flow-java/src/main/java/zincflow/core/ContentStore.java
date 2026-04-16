package zincflow.core;

/// Content-store surface: FlowFile payloads above the claim threshold
/// get offloaded here and referenced by a {@link ClaimContent}. Keeps
/// large payloads off the heap while still letting them ride through
/// the pipeline as a single value.
///
/// Implementations must be thread-safe — multiple pipeline threads can
/// store/retrieve concurrently.
public interface ContentStore {

    /// Store a blob; return a stable claim id that can later be used to
    /// {@link #retrieve} the same bytes.
    String store(byte[] data);

    /// Return the bytes previously stored under {@code claimId}, or an
    /// empty array if the claim is unknown (deleted, or never existed).
    byte[] retrieve(String claimId);

    /// Remove a claim. No-op if the claim doesn't exist.
    void delete(String claimId);

    boolean exists(String claimId);
}
