package caravanflow.processors;

import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RecordContent;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Partition each incoming RecordContent's records across named routes
/// based on per-route expression predicates evaluated against the record's
/// fields. First matching route wins; records matching no route are
/// emitted to {@code unmatched}.
///
/// Mirrors caravan-flow-csharp's {@code RouteRecord}
/// (StdLib/ExpressionProcessors.cs) — the record-level counterpart to
/// {@code RouteOnAttribute}. Config key {@code routes} is a semicolon-
/// delimited list of {@code name: expression} pairs. Each expression
/// evaluates via Apache Commons JEXL with every record field exposed as
/// a top-level variable plus the full map available as {@code record}.
public final class RouteRecord implements Processor {

    private static final JexlEngine JEXL = new JexlBuilder()
            .strict(false).safe(true).silent(false).create();

    private record Route(String name, JexlExpression predicate) {}

    private final List<Route> routes;

    public RouteRecord(String spec) {
        List<Route> parsed = new ArrayList<>();
        if (spec != null) {
            String[] entries = spec.split(";");
            for (int i = 0; i < entries.length; i++) {
                String entry = entries[i].trim();
                if (entry.isEmpty()) continue;
                int colon = entry.indexOf(':');
                if (colon <= 0) {
                    throw new IllegalArgumentException(
                            "RouteRecord: malformed route at index " + i + ": '" + entry
                                    + "' — expected 'name: expression'");
                }
                String name = entry.substring(0, colon).trim();
                String exprStr = entry.substring(colon + 1).trim();
                if (name.isEmpty()) {
                    throw new IllegalArgumentException(
                            "RouteRecord: route at index " + i + " has empty name");
                }
                if (exprStr.isEmpty()) {
                    throw new IllegalArgumentException(
                            "RouteRecord: route '" + name + "' has empty expression");
                }
                if ("unmatched".equals(name)) {
                    throw new IllegalArgumentException(
                            "RouteRecord: 'unmatched' is reserved for records that match no route");
                }
                try {
                    parsed.add(new Route(name, JEXL.createExpression(exprStr)));
                } catch (JexlException ex) {
                    throw new IllegalArgumentException(
                            "RouteRecord: route '" + name + "' has invalid expression '"
                                    + exprStr + "': " + ex.getMessage(), ex);
                }
            }
        }
        this.routes = List.copyOf(parsed);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc)) {
            return ProcessorResult.single(ff);
        }

        // Partition in insertion order so downstream emission order is
        // deterministic across runs.
        Map<String, List<Map<String, Object>>> buckets = new LinkedHashMap<>();
        for (Map<String, Object> record : rc.records()) {
            String matched = null;
            for (Route route : routes) {
                MapContext ctx = new MapContext();
                ctx.set("record", record);
                for (var e : record.entrySet()) ctx.set(e.getKey(), e.getValue());
                Object v;
                try { v = route.predicate().evaluate(ctx); }
                // A record missing a referenced field becomes null under safe
                // JEXL; the compare then returns null. Either way we treat
                // the record as not matching this route, not as a whole-
                // FlowFile failure.
                catch (JexlException ex) { continue; }
                if (isTruthy(v)) { matched = route.name(); break; }
            }
            String key = matched == null ? "unmatched" : matched;
            buckets.computeIfAbsent(key, _ -> new ArrayList<>()).add(record);
        }

        if (buckets.isEmpty()) return ProcessorResult.dropped();

        List<ProcessorResult.MultiRouted.Entry> entries = new ArrayList<>(buckets.size());
        for (var e : buckets.entrySet()) {
            RecordContent child = new RecordContent(e.getValue(), rc.schema());
            entries.add(new ProcessorResult.MultiRouted.Entry(e.getKey(), ff.withContent(child)));
        }
        return ProcessorResult.multiRouted(entries);
    }

    private static boolean isTruthy(Object v) {
        if (v == null) return false;
        if (v instanceof Boolean b) return b;
        if (v instanceof Number n) return n.doubleValue() != 0.0;
        if (v instanceof String s) return !s.isEmpty();
        return true;
    }
}
