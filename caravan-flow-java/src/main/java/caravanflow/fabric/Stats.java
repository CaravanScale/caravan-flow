package caravanflow.fabric;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/// Running counters for the pipeline. Updated on every processor
/// dispatch, surfaced through {@code /api/stats}. Every increment also
/// updates the paired {@link Metrics} registry so {@code /metrics}
/// stays in sync — Stats always holds a non-null registry (defaults to
/// a fresh {@link Metrics} when one isn't supplied).
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
        this.metrics = Objects.requireNonNullElseGet(metrics, Metrics::new);
    }

    public Metrics metrics() { return metrics; }

    void recordIngested() {
        totalIngested.incrementAndGet();
        metrics.recordIngested();
    }

    void recordProcessed(String proc) {
        totalProcessed.incrementAndGet();
        processorCounts.computeIfAbsent(proc, _ -> new AtomicLong()).incrementAndGet();
        metrics.recordProcessed(proc);
    }

    void recordDropped() {
        totalDropped.incrementAndGet();
        metrics.recordDropped();
    }

    void recordFailed(String proc) {
        totalFailed.incrementAndGet();
        processorErrors.computeIfAbsent(proc, _ -> new AtomicLong()).incrementAndGet();
        metrics.recordFailed(proc);
    }

    public Map<String, Long> processorCountsSnapshot() { return mapSnapshot(processorCounts); }
    public Map<String, Long> processorErrorsSnapshot() { return mapSnapshot(processorErrors); }

    /// Zero the per-processor counters for a single processor. Used by
    /// {@code POST /api/processors/{name}/stats/reset}. Leaves pipeline-wide
    /// totals untouched. Returns true if the processor had any counters to
    /// reset (always true for processors that have ever been executed).
    public boolean resetProcessor(String proc) {
        boolean had = false;
        AtomicLong c = processorCounts.get(proc);
        if (c != null) { c.set(0); had = true; }
        AtomicLong e = processorErrors.get(proc);
        if (e != null) { e.set(0); had = true; }
        // Seed zero entries so the snapshot shows the processor even pre-run.
        processorCounts.computeIfAbsent(proc, _ -> new AtomicLong());
        processorErrors.computeIfAbsent(proc, _ -> new AtomicLong());
        return had;
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
