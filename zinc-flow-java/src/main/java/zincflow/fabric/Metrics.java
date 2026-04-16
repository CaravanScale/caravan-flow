package zincflow.fabric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.util.concurrent.ConcurrentHashMap;

/// Prometheus-backed metrics. Mirrors the {@link Stats} counters so the
/// /metrics endpoint and /api/stats stay aligned. Per-processor counters
/// use a tag ({@code processor}) rather than distinct metric names,
/// matching Prometheus idioms.
public final class Metrics {

    private final PrometheusMeterRegistry registry;

    private final Counter ingested;
    private final Counter dropped;

    // Per-processor counters are created on demand to avoid unbounded
    // growth when processors come and go via hot reload.
    private final ConcurrentHashMap<String, Counter> processorCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> processorErrors = new ConcurrentHashMap<>();

    public Metrics() {
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        this.ingested = Counter.builder("zincflow_ingested_total")
                .description("Total FlowFiles accepted at any source")
                .register(registry);
        this.dropped = Counter.builder("zincflow_dropped_total")
                .description("Total FlowFiles dropped by Dropped results or unrouteable failures")
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
                Counter.builder("zincflow_processor_processed_total")
                        .description("FlowFiles dispatched to processor")
                        .tags(Tags.of("processor", p))
                        .register(registry)).increment();
    }
    void recordFailed(String processor) {
        processorErrors.computeIfAbsent(processor, p ->
                Counter.builder("zincflow_processor_errors_total")
                        .description("Processor invocations that threw or returned Failure")
                        .tags(Tags.of("processor", p))
                        .register(registry)).increment();
    }
}
