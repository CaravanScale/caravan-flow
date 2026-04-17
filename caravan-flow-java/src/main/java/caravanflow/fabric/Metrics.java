package caravanflow.fabric;

import caravanflow.core.Source;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/// Prometheus-backed metrics. Mirrors the {@link Stats} counters so the
/// /metrics endpoint and /api/stats stay aligned. Per-processor counters
/// use a tag ({@code processor}) rather than distinct metric names,
/// matching Prometheus idioms.
///
/// Metric names track caravan-flow-csharp's {@code CaravanFlow/Fabric/Metrics.cs}
/// so Prometheus scrapers work against either implementation unchanged.
public final class Metrics {

    private final PrometheusMeterRegistry registry;

    private final Counter ingested;
    private final Counter dropped;

    // Per-processor counters are created on demand to avoid unbounded
    // growth when processors come and go via hot reload.
    private final ConcurrentHashMap<String, Counter> processorCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> processorErrors = new ConcurrentHashMap<>();

    // Gauges registered by Micrometer read through these refs; holding
    // them strongly is required (Micrometer's gauge binding uses weak
    // references on the target object).
    private final AtomicInteger activeExecutions = new AtomicInteger();
    private final Instant startInstant = Instant.now();

    // Remember which sources we've already bound a gauge for so hot
    // reload / repeated addSource calls don't double-register.
    private final Set<String> sourceGaugesBound = ConcurrentHashMap.newKeySet();

    public Metrics() {
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        this.ingested = Counter.builder("caravan_flow_ingested_total")
                .description("Total FlowFiles accepted at any source")
                .register(registry);
        this.dropped = Counter.builder("caravan_flow_dropped_total")
                .description("Total FlowFiles dropped by Dropped results or unrouteable failures")
                .register(registry);
        Gauge.builder("caravan_flow_active_executions", activeExecutions, a -> (double) a.get())
                .description("In-flight pipeline executions")
                .register(registry);
        Gauge.builder("caravan_flow_uptime_seconds", startInstant,
                        s -> (Instant.now().toEpochMilli() - s.toEpochMilli()) / 1000.0)
                .description("Seconds since the worker booted")
                .register(registry);
    }

    /// Expose the registry for Stats' write-through and for alternative
    /// scrape endpoints (e.g. micrometer's built-in binders).
    public PrometheusMeterRegistry registry() {
        return registry;
    }

    public String scrape() {
        return registry.scrape();
    }

    void recordIngested()       { ingested.increment(); }
    void recordDropped()        { dropped.increment(); }
    void recordProcessed(String processor) {
        processorCounts.computeIfAbsent(processor, p ->
                Counter.builder("caravan_flow_processor_processed_total")
                        .description("FlowFiles dispatched to processor")
                        .tags(Tags.of("processor", p))
                        .register(registry)).increment();
    }
    void recordFailed(String processor) {
        processorErrors.computeIfAbsent(processor, p ->
                Counter.builder("caravan_flow_processor_errors_total")
                        .description("Processor invocations that threw or returned Failure")
                        .tags(Tags.of("processor", p))
                        .register(registry)).increment();
    }

    /// Called at the top of {@link Pipeline#ingest}. Paired with
    /// {@link #endExecution()} in a finally block so the gauge tracks
    /// truly in-flight executions even when ingest throws.
    void beginExecution() { activeExecutions.incrementAndGet(); }
    void endExecution()   { activeExecutions.decrementAndGet(); }

    /// Current in-flight execution count. Surfaced through
    /// {@code /api/stats} and {@code /api/flow}'s embedded stats so
    /// operators can see live load without waiting for a Prometheus
    /// scrape round-trip.
    public int activeExecutions() { return activeExecutions.get(); }

    /// Bind a running-state gauge for a source. The gauge reads
    /// {@link Source#isRunning()} at scrape time, so start/stop
    /// transitions don't need explicit metric updates.
    void onSourceRegistered(Source source) {
        if (source == null) return;
        String name = source.name();
        if (name == null || !sourceGaugesBound.add(name)) return;
        Gauge.builder("caravan_flow_source_running", source, s -> s.isRunning() ? 1.0 : 0.0)
                .description("1 when the source is actively polling, 0 when stopped")
                .tags(Tags.of("name", name, "type", source.sourceType() == null ? "unknown" : source.sourceType()))
                .register(registry);
    }
}
