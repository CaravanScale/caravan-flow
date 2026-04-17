package caravanflow.ui.views;

import io.javalin.http.Context;
import io.pebbletemplates.pebble.PebbleEngine;
import io.pebbletemplates.pebble.template.PebbleTemplate;
import caravanflow.shared.Identity;
import caravanflow.ui.FleetService;

import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;

/// Handler for the /nodes page. Phase 1 placeholder — shows the
/// single worker's identity and a note that multi-node fleet views
/// arrive in Phase 2.
public final class NodesController {

    private final FleetService fleet;
    private final PebbleEngine pebble;

    public NodesController(FleetService fleet, PebbleEngine pebble) {
        this.fleet = fleet;
        this.pebble = pebble;
    }

    public void handleNodes(Context ctx) throws Exception {
        Map<String, Object> model = new LinkedHashMap<>();
        model.put("workerUrl", fleet.workerBaseUrl().toString());
        try {
            Identity id = fleet.identity();
            model.put("identity", identityToMap(id));
        } catch (RuntimeException ex) {
            model.put("identityError", ex.getMessage());
        }
        PebbleTemplate t = pebble.getTemplate("templates/nodes.peb");
        StringWriter sw = new StringWriter();
        t.evaluate(sw, model);
        ctx.contentType("text/html; charset=utf-8").result(sw.toString());
    }

    private static Map<String, Object> identityToMap(Identity id) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("nodeId",       id.nodeId());
        out.put("hostname",     id.hostname());
        out.put("version",      id.version());
        out.put("port",         id.port());
        out.put("uptimeMillis", id.uptimeMillis());
        return out;
    }
}
