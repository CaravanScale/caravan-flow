package zincflow.fabric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

/// Direct pipeline executor — iterative, depth-first, work-stack. No
/// inter-stage queues. Each call to {@link #ingest(FlowFile)} runs the
/// FlowFile through the graph on the calling thread; concurrency comes
/// from having multiple source threads call {@code ingest} in parallel.
///
/// Matches the zinc-flow-csharp model — see
/// {@code zinc-flow-csharp/ZincFlow/Fabric/Fabric.cs}.
public final class Pipeline {

    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);

    /// Hard cap on processor dispatches per FlowFile — catches accidental
    /// cycles in the graph so a pipeline misconfiguration can't spin
    /// forever. Matches the C# default.
    public static final int DEFAULT_MAX_HOPS = 50;

    private volatile PipelineGraph graph;
    private final Stats stats;
    private final Metrics metrics;
    private final int maxHops;

    public Pipeline(PipelineGraph graph) {
        this(graph, DEFAULT_MAX_HOPS, null);
    }

    public Pipeline(PipelineGraph graph, int maxHops) {
        this(graph, maxHops, null);
    }

    public Pipeline(PipelineGraph graph, int maxHops, Metrics metrics) {
        this.graph = Objects.requireNonNull(graph);
        this.maxHops = maxHops;
        this.metrics = metrics;
        this.stats = new Stats(metrics);
    }

    public Metrics metrics() {
        return metrics;
    }

    /// Swap the graph atomically. In-flight ingest calls complete against
    /// whichever reference they already loaded; subsequent ingests see
    /// the new graph. This is the hot-reload hook.
    public void swapGraph(PipelineGraph next) {
        this.graph = Objects.requireNonNull(next);
    }

    public Stats stats() {
        return stats;
    }

    public PipelineGraph graph() {
        return graph;
    }

    /// Push a FlowFile into the pipeline at each entry point. Returns
    /// when all downstream dispatches for this FlowFile have completed.
    public void ingest(FlowFile ff) {
        PipelineGraph g = graph; // single snapshot for this call
        if (g.entryPoints().isEmpty()) {
            log.warn("ingest called but pipeline has no entry points — dropping {}", ff.stringId());
            return;
        }
        stats.recordIngested();
        // A work item = (processor name, flowfile to hand it). Stack is
        // local per ingest call so concurrent ingests don't share state.
        Deque<WorkItem> stack = new ArrayDeque<>();
        for (String entry : g.entryPoints()) {
            stack.push(new WorkItem(entry, ff));
        }
        drain(g, stack);
    }

    private void drain(PipelineGraph g, Deque<WorkItem> stack) {
        while (!stack.isEmpty()) {
            WorkItem item = stack.pop();
            FlowFile input = item.flowFile;

            if (input.hopCount() >= maxHops) {
                log.error("maxHops={} exceeded at processor={}, dropping {}",
                        maxHops, item.processor, input.stringId());
                stats.recordFailed(item.processor);
                continue;
            }
            Processor processor = g.processors().get(item.processor);
            if (processor == null) {
                log.error("unknown processor '{}' referenced by graph — dropping {}",
                        item.processor, input.stringId());
                stats.recordFailed(item.processor);
                continue;
            }

            ProcessorResult result;
            try {
                result = processor.process(input);
            } catch (RuntimeException ex) {
                log.error("processor '{}' threw while handling {}: {}",
                        item.processor, input.stringId(), ex.toString(), ex);
                stats.recordFailed(item.processor);
                dispatchFailure(g, stack, item.processor, input, ex.getMessage());
                continue;
            }
            stats.recordProcessed(item.processor);
            dispatch(g, stack, item.processor, result);
        }
    }

    private void dispatch(PipelineGraph g, Deque<WorkItem> stack,
                          String from, ProcessorResult result) {
        switch (result) {
            case ProcessorResult.Single(var ff) ->
                fanOut(g, stack, from, "success", List.of(ff.bumpHop()));
            case ProcessorResult.Multiple(var ffs) -> {
                for (FlowFile out : ffs) {
                    fanOut(g, stack, from, "success", List.of(out.bumpHop()));
                }
            }
            case ProcessorResult.Routed(var route, var ff) ->
                fanOut(g, stack, from, route, List.of(ff.bumpHop()));
            case ProcessorResult.Dropped d -> stats.recordDropped();
            case ProcessorResult.Failure(var reason, var ff) ->
                dispatchFailure(g, stack, from, ff, reason);
        }
    }

    private void dispatchFailure(PipelineGraph g, Deque<WorkItem> stack,
                                 String from, FlowFile ff, String reason) {
        List<String> failureTargets = g.next(from, "failure");
        if (failureTargets.isEmpty()) {
            log.warn("failure at '{}' with no 'failure' connections — dropping {} (reason: {})",
                    from, ff.stringId(), reason);
            stats.recordDropped();
            return;
        }
        fanOut(g, stack, from, "failure", List.of(ff.bumpHop()));
    }

    private void fanOut(PipelineGraph g, Deque<WorkItem> stack,
                        String from, String relationship, List<FlowFile> ffs) {
        List<String> targets = g.next(from, relationship);
        if (targets.isEmpty()) {
            return; // sink / terminal branch
        }
        for (String target : targets) {
            for (FlowFile ff : ffs) {
                stack.push(new WorkItem(target, ff));
            }
        }
    }

    private record WorkItem(String processor, FlowFile flowFile) {}
}
