package caravanflow.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/// In-process {@link ContentStore} — holds every claim in a
/// {@link ConcurrentHashMap}. Used by tests and by production pipelines
/// that don't need disk-backed content (the bytes already live in the
/// JVM anyway).
public final class MemoryContentStore implements ContentStore {

    private final ConcurrentHashMap<String, byte[]> data = new ConcurrentHashMap<>();
    private final AtomicLong counter = new AtomicLong();

    @Override
    public String store(byte[] bytes) {
        if (bytes == null) bytes = new byte[0];
        String id = "mem-claim-" + counter.incrementAndGet();
        data.put(id, bytes);
        return id;
    }

    @Override
    public byte[] retrieve(String claimId) {
        byte[] v = claimId == null ? null : data.get(claimId);
        return v == null ? new byte[0] : v;
    }

    @Override
    public void delete(String claimId) {
        if (claimId != null) data.remove(claimId);
    }

    @Override
    public boolean exists(String claimId) {
        return claimId != null && data.containsKey(claimId);
    }

    public int size() { return data.size(); }
}
