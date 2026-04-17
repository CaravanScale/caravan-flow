package caravanflow.ui.views;

import io.javalin.http.Context;
import io.pebbletemplates.pebble.PebbleEngine;
import io.pebbletemplates.pebble.template.PebbleTemplate;
import caravanflow.shared.FlowSnapshot;
import caravanflow.shared.ProcessorView;
import caravanflow.shared.ProviderView;
import caravanflow.shared.SourceView;
import caravanflow.ui.BfsLayout;
import caravanflow.ui.FleetService;
import caravanflow.ui.UiRoutes;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Handlers for the /flow view. Records don't round-trip through
/// Pebble's default attribute resolver (which expects bean-style
/// getters), so the controller converts the snapshot into plain
/// Maps before handing it off to the template.
public final class FlowController {

    private final FleetService fleet;
    private final PebbleEngine pebble;

    public FlowController(FleetService fleet, PebbleEngine pebble) {
        this.fleet = fleet;
        this.pebble = pebble;
    }

    public void handleFlow(Context ctx) throws Exception {
        ctx.contentType("text/html; charset=utf-8").result(render("flow.peb", baseModel()));
    }

    public void handleFlowCards(Context ctx) throws Exception {
        ctx.contentType("text/html; charset=utf-8").result(render("flow-cards.peb", baseModel()));
    }

    private Map<String, Object> baseModel() {
        Map<String, Object> model = new LinkedHashMap<>();
        model.put("workerUrl", fleet.workerBaseUrl().toString());
        model.put("flowCardsUrl", UiRoutes.FLOW_CARDS);
        try {
            FlowSnapshot snap = fleet.flow();
            model.put("flow", flowToMap(snap));
            model.put("layout", layoutToMap(BfsLayout.of(snap)));
        } catch (RuntimeException ex) {
            model.put("flowError", ex.getMessage());
        }
        return model;
    }

    private static Map<String, Object> flowToMap(FlowSnapshot snap) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("entryPoints", snap.entryPoints());
        out.put("processors",  snap.processors().stream().map(FlowController::procToMap).toList());
        out.put("connections", snap.connections());
        out.put("providers",   snap.providers().stream().map(FlowController::providerToMap).toList());
        out.put("sources",     snap.sources().stream().map(FlowController::sourceToMap).toList());
        out.put("stats",       snap.stats());
        return out;
    }

    private static Map<String, Object> procToMap(ProcessorView p) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("name",        p.name());
        out.put("type",        p.type());
        out.put("state",       p.state());
        out.put("stats",       p.stats());
        out.put("connections", p.connections());
        return out;
    }

    private static Map<String, Object> providerToMap(ProviderView p) {
        return Map.of("name", p.name(), "type", p.type(), "state", p.state());
    }

    private static Map<String, Object> sourceToMap(SourceView s) {
        return Map.of("name", s.name(), "type", s.type(), "running", s.running());
    }

    private static Map<String, Object> layoutToMap(BfsLayout layout) {
        Map<String, Object> out = new LinkedHashMap<>();
        List<List<Map<String, Object>>> cols = new ArrayList<>();
        for (List<ProcessorView> col : layout.columns()) {
            List<Map<String, Object>> colMaps = new ArrayList<>();
            for (ProcessorView p : col) colMaps.add(procToMap(p));
            cols.add(colMaps);
        }
        out.put("columns", cols);
        out.put("unreachable", layout.unreachable().stream().map(FlowController::procToMap).toList());
        return out;
    }

    private String render(String template, Map<String, Object> model) throws Exception {
        PebbleTemplate t = pebble.getTemplate("templates/" + template);
        StringWriter sw = new StringWriter();
        t.evaluate(sw, model);
        return sw.toString();
    }
}
