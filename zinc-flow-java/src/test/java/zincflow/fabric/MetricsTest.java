package zincflow.fabric;

import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class MetricsTest {

    @Test
    void scrapeContainsProcessorCounter() {
        Processor noop = ff -> ProcessorResult.dropped();
        var graph = new PipelineGraph(Map.of("noop", noop), Map.of(), List.of("noop"));
        var metrics = new Metrics();
        var pipeline = new Pipeline(graph, Pipeline.DEFAULT_MAX_HOPS, metrics);

        pipeline.ingest(FlowFile.create(new byte[0], Map.of()));
        pipeline.ingest(FlowFile.create(new byte[0], Map.of()));

        String scrape = metrics.scrape();
        assertTrue(scrape.contains("zincflow_ingested_total"),
                "expected zincflow_ingested_total in metrics, got:\n" + scrape);
        assertTrue(scrape.contains("processor=\"noop\""),
                "expected per-processor tag in scrape output, got:\n" + scrape);
        assertTrue(scrape.contains("zincflow_dropped_total 2"),
                "dropped should equal ingest count when sole processor returns Dropped, got:\n" + scrape);
    }

    @Test
    void statsAndMetricsAgree() {
        Processor noop = ff -> ProcessorResult.single(ff);
        var graph = new PipelineGraph(Map.of("noop", noop), Map.of(), List.of("noop"));
        var metrics = new Metrics();
        var pipeline = new Pipeline(graph, Pipeline.DEFAULT_MAX_HOPS, metrics);

        for (int i = 0; i < 5; i++) {
            pipeline.ingest(FlowFile.create(new byte[0], Map.of()));
        }

        var snapshot = pipeline.stats().snapshot();
        assertEquals(5L, snapshot.get("totalIngested"));
        assertEquals(5L, snapshot.get("totalProcessed"));
        assertTrue(metrics.scrape().contains("zincflow_ingested_total 5"),
                "metrics registry should reflect 5 ingests");
    }

    @Test
    void defaultPipelineHasMetrics() {
        // Pipeline constructed without an explicit Metrics still has one —
        // a fresh registry is allocated so /metrics always works and the
        // call sites never need to null-check.
        Processor noop = ff -> ProcessorResult.dropped();
        var graph = new PipelineGraph(Map.of("noop", noop), Map.of(), List.of("noop"));
        var pipeline = new Pipeline(graph);
        assertNotNull(pipeline.metrics());
        assertDoesNotThrow(() -> pipeline.ingest(FlowFile.create(new byte[0], Map.of())));
        assertTrue(pipeline.metrics().scrape().contains("zincflow_ingested_total 1"));
    }
}
