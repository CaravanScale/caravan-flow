package zincflow.ui.views;

import io.javalin.http.Context;
import io.pebbletemplates.pebble.PebbleEngine;
import io.pebbletemplates.pebble.template.PebbleTemplate;
import zincflow.shared.OverlayInfo;
import zincflow.ui.FleetService;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Handler for the /settings page. Read-only view of the worker's
/// overlay stack in Phase 1 — edit forms come in Phase 3.
public final class SettingsController {

    private final FleetService fleet;
    private final PebbleEngine pebble;

    public SettingsController(FleetService fleet, PebbleEngine pebble) {
        this.fleet = fleet;
        this.pebble = pebble;
    }

    public void handleSettings(Context ctx) throws Exception {
        Map<String, Object> model = new LinkedHashMap<>();
        model.put("workerUrl", fleet.workerBaseUrl().toString());
        try {
            OverlayInfo overlays = fleet.overlays();
            model.put("overlays", overlaysToMap(overlays));
        } catch (RuntimeException ex) {
            model.put("overlaysError", ex.getMessage());
        }
        PebbleTemplate t = pebble.getTemplate("templates/settings.peb");
        StringWriter sw = new StringWriter();
        t.evaluate(sw, model);
        ctx.contentType("text/html; charset=utf-8").result(sw.toString());
    }

    private static Map<String, Object> overlaysToMap(OverlayInfo o) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("base", o.base());
        List<Map<String, Object>> layers = new ArrayList<>();
        for (OverlayInfo.Layer layer : o.layers()) {
            Map<String, Object> l = new LinkedHashMap<>();
            l.put("role",    layer.role());
            l.put("path",    layer.path());
            l.put("present", layer.present());
            l.put("size",    layer.size());
            layers.add(l);
        }
        out.put("layers", layers);
        out.put("effective",  o.effective()  == null ? Map.of() : o.effective());
        out.put("provenance", o.provenance() == null ? Map.of() : o.provenance());
        return out;
    }
}
