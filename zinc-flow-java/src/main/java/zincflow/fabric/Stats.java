package zincflow.fabric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/// Running counters for the pipeline. Updated on every processor
/// dispatch, surfaced through {@code /api/stats}. When a
/// {@link Metrics} registry is wired in, every increment also updates
/// the Prometheus-backed counter so /metrics stays in sync.
public final class Stats {

    private final AtomicLong totalIngested = new AtomicLong();
    private final AtomicLong totalProcessed = new AtomicLong();
    private final AtomicLong totalDropped = new AtomicLong();
    private final AtomicLong totalFailed = new AtomicLong();
    private final ConcurrentHashMap<String, AtomicLong> processorCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> processorErrors = new ConcurrentHashMap<>();

    private final Metrics metrics;

    public Stats() { this(null); }

    public Stats(Metrics metrics) {
        this.metrics = metrics;
    }

    void recordIngested() {
        totalIngested.incrementAndGet();
        if (metrics != null) metrics.recordIngested();
    }

    void recordProcessed(String proc) {
        totalProcessed.incrementAndGet();
        processorCounts.computeIfAbsent(proc, _ -> new AtomicLong()).incrementAndGet();
        if (metrics != null) metrics.recordProcessed(proc);
    }

    void recordDropped() {
        totalDropped.incrementAndGet();
        if (metrics != null) metrics.recordDropped();
    }

    void recordFailed(String proc) {
        totalFailed.incrementAndGet();
        processorErrors.computeIfAbsent(proc, _ -> new AtomicLong()).incrementAndGet();
        if (metrics != null) metrics.recordFailed(proc);
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
