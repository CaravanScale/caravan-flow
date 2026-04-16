package zincflow.fabric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/// Running counters for the pipeline. Updated on every processor
/// dispatch, surfaced through {@code /api/stats}.
public final class Stats {

    private final AtomicLong totalIngested = new AtomicLong();
    private final AtomicLong totalProcessed = new AtomicLong();
    private final AtomicLong totalDropped = new AtomicLong();
    private final AtomicLong totalFailed = new AtomicLong();
    private final ConcurrentHashMap<String, AtomicLong> processorCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> processorErrors = new ConcurrentHashMap<>();

    void recordIngested()       { totalIngested.incrementAndGet(); }
    void recordProcessed(String proc) {
        totalProcessed.incrementAndGet();
        processorCounts.computeIfAbsent(proc, _ -> new AtomicLong()).incrementAndGet();
    }
    void recordDropped()        { totalDropped.incrementAndGet(); }
    void recordFailed(String proc) {
        totalFailed.incrementAndGet();
        processorErrors.computeIfAbsent(proc, _ -> new AtomicLong()).incrementAndGet();
    }

    /// Snapshot suitable for JSON serialization through Jackson.
    public Map<String, Object> snapshot() {
        return Map.of(
                "totalIngested",    totalIngested.get(),
                "totalProcessed",   totalProcessed.get(),
                "totalDropped",     totalDropped.get(),
                "totalFailed",      totalFailed.get(),
                "processorCounts",  mapSnapshot(processorCounts),
                "processorErrors",  mapSnapshot(processorErrors));
    }

    private static Map<String, Long> mapSnapshot(ConcurrentHashMap<String, AtomicLong> src) {
        ConcurrentHashMap<String, Long> out = new ConcurrentHashMap<>(src.size());
        src.forEach((k, v) -> out.put(k, v.get()));
        return Map.copyOf(out);
    }
}
