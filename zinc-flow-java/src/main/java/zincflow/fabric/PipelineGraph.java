package zincflow.fabric;

import zincflow.core.Processor;

import java.util.List;
import java.util.Map;

/// Immutable description of the processor DAG. Built from config.yaml
/// (see {@link zincflow.fabric.ConfigLoader}) and passed to
/// {@link Pipeline} for execution. Swap the whole graph atomically
/// for hot reload.
///
/// @param processors  processor name → processor instance
/// @param connections fromProcessor → relationship (e.g. "success",
///                    "failure", "matched") → list of target processor names
/// @param entryPoints processor names that receive fresh FlowFiles from
///                    sources (top of the DAG)
public record PipelineGraph(
        Map<String, Processor> processors,
        Map<String, Map<String, List<String>>> connections,
        List<String> entryPoints) {

    public PipelineGraph {
        processors = Map.copyOf(processors);
        // Deep-copy connections to make the map truly immutable.
        connections = Map.copyOf(connections);
        entryPoints = List.copyOf(entryPoints);
    }

    public static PipelineGraph empty() {
        return new PipelineGraph(Map.of(), Map.of(), List.of());
    }

    /// Next processor names reachable from {@code fromProcessor} along
    /// the given {@code relationship}. Empty list if none — the executor
    /// treats that as "terminal for this branch."
    public List<String> next(String fromProcessor, String relationship) {
        Map<String, List<String>> outgoing = connections.get(fromProcessor);
        if (outgoing == null) return List.of();
        List<String> targets = outgoing.get(relationship);
        return targets == null ? List.of() : targets;
    }
}
