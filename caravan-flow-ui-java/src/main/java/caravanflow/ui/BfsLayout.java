package caravanflow.ui;

import caravanflow.shared.FlowSnapshot;
import caravanflow.shared.ProcessorView;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Workflow-style (n8n / Step Functions) layout for a FlowSnapshot.
/// Produces columns ordered by BFS depth from the entry points:
/// column 0 holds the entry-point processors, column 1 their direct
/// successors, and so on. Matches the design doc's locked stance
/// that the data structure IS the layout — no positions persisted.
///
/// Unreachable processors (present in the graph but not visited from
/// any entry point — possible after a config edit that leaves
/// orphans) land in the trailing {@link #unreachable} bucket so the
/// UI can surface them without hiding them.
///
/// Edges are emitted once per (from, relationship, to) triple in the
/// order they appear in the snapshot's connection map, so tests can
/// pin down the deterministic rendering sequence.
public final class BfsLayout {

    public record Edge(String from, String relationship, String to) {}

    private final List<List<ProcessorView>> columns;
    private final List<ProcessorView> unreachable;
    private final List<Edge> edges;

    private BfsLayout(List<List<ProcessorView>> columns,
                      List<ProcessorView> unreachable,
                      List<Edge> edges) {
        this.columns = columns;
        this.unreachable = unreachable;
        this.edges = edges;
    }

    public List<List<ProcessorView>> columns() { return columns; }
    public List<ProcessorView> unreachable()   { return unreachable; }
    public List<Edge> edges()                  { return edges; }

    public static BfsLayout of(FlowSnapshot snap) {
        Map<String, ProcessorView> byName = new LinkedHashMap<>();
        if (snap.processors() != null) {
            for (ProcessorView p : snap.processors()) byName.put(p.name(), p);
        }

        Map<String, Integer> depth = bfsDepth(snap, byName.keySet());

        int maxDepth = depth.values().stream().mapToInt(Integer::intValue).max().orElse(-1);
        List<List<ProcessorView>> cols = new ArrayList<>(Math.max(maxDepth + 1, 0));
        for (int i = 0; i <= maxDepth; i++) cols.add(new ArrayList<>());

        // Preserve processor declaration order within each column.
        for (ProcessorView p : byName.values()) {
            Integer d = depth.get(p.name());
            if (d != null) cols.get(d).add(p);
        }

        List<ProcessorView> orphans = new ArrayList<>();
        for (ProcessorView p : byName.values()) {
            if (!depth.containsKey(p.name())) orphans.add(p);
        }

        List<Edge> edgeList = new ArrayList<>();
        if (snap.connections() != null) {
            for (var fromEntry : snap.connections().entrySet()) {
                String from = fromEntry.getKey();
                if (fromEntry.getValue() == null) continue;
                for (var relEntry : fromEntry.getValue().entrySet()) {
                    String rel = relEntry.getKey();
                    List<String> targets = relEntry.getValue();
                    if (targets == null) continue;
                    for (String to : targets) edgeList.add(new Edge(from, rel, to));
                }
            }
        }

        return new BfsLayout(List.copyOf(cols), List.copyOf(orphans), List.copyOf(edgeList));
    }

    private static Map<String, Integer> bfsDepth(FlowSnapshot snap, java.util.Set<String> known) {
        Map<String, Integer> depth = new HashMap<>();
        Deque<String> queue = new ArrayDeque<>();
        if (snap.entryPoints() != null) {
            for (String ep : snap.entryPoints()) {
                if (!known.contains(ep)) continue;
                if (depth.putIfAbsent(ep, 0) == null) queue.add(ep);
            }
        }
        Map<String, Map<String, List<String>>> conns = snap.connections() == null
                ? Map.of() : snap.connections();
        while (!queue.isEmpty()) {
            String from = queue.poll();
            int d = depth.get(from);
            Map<String, List<String>> outgoing = conns.get(from);
            if (outgoing == null) continue;
            for (List<String> targets : outgoing.values()) {
                if (targets == null) continue;
                for (String t : targets) {
                    if (!known.contains(t)) continue;
                    if (depth.putIfAbsent(t, d + 1) == null) queue.add(t);
                }
            }
        }
        return depth;
    }
}
