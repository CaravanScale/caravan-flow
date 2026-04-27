package zincflow.fabric;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/// Static DAG check over a processor graph. Three classes of finding:
/// <ul>
///   <li><b>errors</b> — unknown connection targets. Load must abort.</li>
///   <li><b>warnings</b> — cycles, unreachable processors, no entry
///       points. Non-fatal; surfaced to the operator.</li>
///   <li><b>entry points</b> — processors with no inbound connections,
///       so the executor knows where to inject ingested FlowFiles.</li>
/// </ul>
///
/// Port of zinc-flow-csharp's DagValidator — called from
/// {@link ConfigLoader} after processors and connections are built so
/// a misconfigured config surfaces every problem in one go rather than
/// the first-one-wins style of the previous reference-existence check.
public final class FlowValidator {

    private FlowValidator() {}

    public record Result(List<String> errors, List<String> warnings, List<String> entryPoints) {
        public Result {
            errors = List.copyOf(errors);
            warnings = List.copyOf(warnings);
            entryPoints = List.copyOf(entryPoints);
        }

        public boolean ok() { return errors.isEmpty(); }
    }

    /// Validate the processor DAG. {@code processorNames} is the full
    /// set of defined processors (so the unreachable check can warn
    /// on sink processors declared but never wired); {@code connections}
    /// maps {@code fromProcessor → relationship → [target,...]}.
    public static Result validate(Collection<String> processorNames,
                                  Map<String, Map<String, List<String>>> connections) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        Set<String> all = new LinkedHashSet<>(processorNames);

        // Build adjacency (flatten relationships) + collect referenced targets.
        Map<String, List<String>> adjacency = new HashMap<>();
        Set<String> referenced = new HashSet<>();
        for (String proc : all) {
            Map<String, List<String>> rels = connections.getOrDefault(proc, Map.of());
            List<String> targets = new ArrayList<>();
            for (List<String> dests : rels.values()) {
                for (String dest : dests) {
                    targets.add(dest);
                    referenced.add(dest);
                    if (!all.contains(dest)) {
                        errors.add("processor '" + proc + "' connects to unknown target '" + dest + "'");
                    }
                }
            }
            adjacency.put(proc, targets);
        }

        // Entry points: processors nobody targets.
        List<String> entryPoints = new ArrayList<>();
        for (String proc : all) if (!referenced.contains(proc)) entryPoints.add(proc);
        if (entryPoints.isEmpty() && !all.isEmpty()) {
            warnings.add("no entry-point processors detected (every processor is also a target)");
        }

        // Cycle detection (DFS 3-color).
        Map<String, Integer> color = new HashMap<>();
        for (String proc : all) color.put(proc, 0);
        List<String> path = new ArrayList<>();
        for (String proc : all) {
            if (color.get(proc) == 0) {
                detectCycles(proc, adjacency, color, path, warnings, all);
            }
        }

        // Unreachable processors: BFS from entry points.
        if (!entryPoints.isEmpty()) {
            Set<String> reachable = new HashSet<>();
            Deque<String> queue = new ArrayDeque<>(entryPoints);
            while (!queue.isEmpty()) {
                String current = queue.poll();
                if (!reachable.add(current)) continue;
                for (String t : adjacency.getOrDefault(current, List.of())) {
                    if (all.contains(t) && !reachable.contains(t)) queue.add(t);
                }
            }
            for (String proc : all) {
                if (!reachable.contains(proc)) {
                    warnings.add("processor '" + proc + "' is not reachable from any entry point");
                }
            }
        }

        return new Result(errors, warnings, entryPoints);
    }

    private static void detectCycles(String node,
                                     Map<String, List<String>> adjacency,
                                     Map<String, Integer> color,
                                     List<String> path,
                                     List<String> warnings,
                                     Set<String> all) {
        color.put(node, 1);
        path.add(node);
        for (String target : adjacency.getOrDefault(node, List.of())) {
            if (!all.contains(target)) continue;
            Integer c = color.get(target);
            if (c == null) continue;
            if (c == 1) {
                // gray = back edge = cycle
                int cycleStart = path.indexOf(target);
                StringBuilder sb = new StringBuilder("cycle detected: ");
                for (int i = cycleStart; i < path.size(); i++) sb.append(path.get(i)).append(" → ");
                sb.append(target);
                warnings.add(sb.toString());
            } else if (c == 0) {
                detectCycles(target, adjacency, color, path, warnings, all);
            }
        }
        path.remove(path.size() - 1);
        color.put(node, 2);
    }
}
