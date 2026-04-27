package zincflow.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/// Content-store surface: FlowFile payloads above the claim threshold
/// get offloaded here and referenced by a {@link ClaimContent}. Keeps
/// large payloads off the heap while still letting them ride through
/// the pipeline as a single value.
///
/// Two I/O surfaces — {@link #retrieve(String)} loads the whole blob
/// into a {@code byte[]} (convenient for small content) and
/// {@link #openRead(String)} streams it (required for multi-GB
/// payloads). Implementations must be thread-safe — multiple pipeline
/// threads can store/retrieve concurrently.
public interface ContentStore {

    /// Store a blob; return a stable claim id that can later be used to
    /// {@link #retrieve} the same bytes.
    String store(byte[] data);

    /// Stream store — useful for large payloads; reads the input fully
    /// without buffering the whole thing in memory. Default delegates
    /// to {@link #store(byte[])} for implementations that don't have a
    /// native streaming path.
    default String store(InputStream in) throws IOException {
        return store(in.readAllBytes());
    }

    /// Return the bytes previously stored under {@code claimId}, or an
    /// empty array if the claim is unknown (deleted, or never existed).
    byte[] retrieve(String claimId);

    /// Stream the claim back. Default wraps {@link #retrieve(String)};
    /// disk-backed stores override to avoid loading the whole blob
    /// into memory.
    default InputStream openRead(String claimId) throws IOException {
        return new ByteArrayInputStream(retrieve(claimId));
    }

    /// Size in bytes of the stored claim, or {@code -1} when the store
    /// doesn't know (e.g. a streaming source that hasn't been counted).
    /// Default reads through {@link #retrieve} and returns its length.
    default long size(String claimId) {
        byte[] b = retrieve(claimId);
        return b.length;
    }

    /// Remove a claim. No-op if the claim doesn't exist.
    void delete(String claimId);

    boolean exists(String claimId);
}
