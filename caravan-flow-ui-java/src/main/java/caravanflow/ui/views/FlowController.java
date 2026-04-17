package caravanflow.ui.views;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.http.Context;
import io.pebbletemplates.pebble.PebbleEngine;
import io.pebbletemplates.pebble.template.PebbleTemplate;
import caravanflow.shared.FlowSnapshot;
import caravanflow.shared.ProcessorView;
import caravanflow.ui.FleetService;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Handlers for /flow, the cytoscape-rendered pipeline view.
///
/// Three endpoints:
///   /flow              — full page, bakes initial graph JSON into
///                        the document so the client doesn't need a
///                        second roundtrip to render.
///   /flow/stats.json   — live per-node state + stats; the client
///                        polls this every 2 s and patches cytoscape
///                        node data in place (no re-layout).
///   /flow/panel/{name} — drawer body for one processor (HTMX
///                        partial — self-refreshes every 2 s).
public final class FlowController {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final FleetService fleet;
    private final PebbleEngine pebble;

    public FlowController(FleetService fleet, PebbleEngine pebble) {
        this.fleet = fleet;
        this.pebble = pebble;
    }

    public void handleFlow(Context ctx) throws Exception {
        Map<String, Object> model = new LinkedHashMap<>();
        model.put("workerUrl", fleet.workerBaseUrl().toString());
        try {
            FlowSnapshot snap = fleet.flow();
            model.put("graphJson", JSON.writeValueAsString(graphFromSnapshot(snap)));
        } catch (RuntimeException ex) {
            // Client-side flow.js reads {error:...} out of the JSON
            // blob and surfaces it in the error banner.
            model.put("flowError", ex.getMessage());
            model.put("graphJson", JSON.writeValueAsString(Map.of("error", ex.getMessage())));
        }
        ctx.contentType("text/html; charset=utf-8").result(render("flow.peb", model));
    }

    public void handleFlowStats(Context ctx) throws Exception {
        try {
            FlowSnapshot snap = fleet.flow();
            List<Map<String, Object>> procs = new ArrayList<>();
            for (ProcessorView p : snap.processors()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("name",  p.name());
                row.put("state", p.state());
                row.put("stats", p.stats() == null ? Map.of() : p.stats());
                procs.add(row);
            }
            ctx.contentType("application/json").result(JSON.writeValueAsBytes(Map.of("processors", procs)));
        } catch (RuntimeException ex) {
            ctx.status(503).contentType("application/json")
               .result(JSON.writeValueAsBytes(Map.of("error", ex.getMessage())));
        }
    }

    public void handleFlowPanel(Context ctx) throws Exception {
        String name = ctx.pathParam("name");
        Map<String, Object> model = new LinkedHashMap<>();
        try {
            FlowSnapshot snap = fleet.flow();
            ProcessorView match = null;
            for (ProcessorView p : snap.processors()) {
                if (p.name().equals(name)) { match = p; break; }
            }
            if (match == null) {
                model.put("panelError", "processor not found: " + name);
                model.put("processor", Map.of(
                        "name", name, "type", "-", "state", "UNKNOWN",
                        "config", Map.of(), "stats", Map.of(), "connections", Map.of()));
            } else {
                model.put("processor", procToMap(match));
            }
        } catch (RuntimeException ex) {
            model.put("panelError", ex.getMessage());
            model.put("processor", Map.of(
                    "name", name, "type", "-", "state", "UNKNOWN",
                    "config", Map.of(), "stats", Map.of(), "connections", Map.of()));
        }
        ctx.contentType("text/html; charset=utf-8").result(render("flow-panel.peb", model));
    }

    // --- graph shaping ---

    private static Map<String, Object> graphFromSnapshot(FlowSnapshot snap) {
        Map<String, Object> out = new LinkedHashMap<>();
        List<Map<String, Object>> procs = new ArrayList<>();
        for (ProcessorView p : snap.processors()) procs.add(procForGraph(p));
        out.put("processors", procs);
        out.put("edges", edgesFromSnapshot(snap));
        return out;
    }

    private static Map<String, Object> procForGraph(ProcessorView p) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("name",  p.name());
        out.put("type",  p.type());
        out.put("state", p.state());
        out.put("stats", p.stats() == null ? Map.of() : p.stats());
        return out;
    }

    private static List<Map<String, Object>> edgesFromSnapshot(FlowSnapshot snap) {
        List<Map<String, Object>> edges = new ArrayList<>();
        int counter = 0;
        for (var entry : snap.connections().entrySet()) {
            String source = entry.getKey();
            Map<String, List<String>> rels = entry.getValue();
            if (rels == null) continue;
            for (var rel : rels.entrySet()) {
                String relationship = rel.getKey();
                List<String> targets = rel.getValue();
                if (targets == null) continue;
                for (String target : targets) {
                    Map<String, Object> edge = new LinkedHashMap<>();
                    edge.put("id",     "e" + (counter++));
                    edge.put("source", source);
                    edge.put("target", target);
                    edge.put("label",  relationship);
                    edges.add(edge);
                }
            }
        }
        return edges;
    }

    private static Map<String, Object> procToMap(ProcessorView p) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("name",        p.name());
        out.put("type",        p.type());
        out.put("state",       p.state());
        out.put("config",      p.config() == null ? Map.of() : p.config());
        out.put("stats",       p.stats() == null ? Map.of() : p.stats());
        out.put("connections", p.connections() == null ? Map.of() : p.connections());
        return out;
    }

    private String render(String template, Map<String, Object> model) throws Exception {
        PebbleTemplate t = pebble.getTemplate("templates/" + template);
        StringWriter sw = new StringWriter();
        t.evaluate(sw, model);
        return sw.toString();
    }
}
