package zincflow.ui.views;

import io.javalin.http.Context;
import io.pebbletemplates.pebble.PebbleEngine;
import io.pebbletemplates.pebble.template.PebbleTemplate;
import zincflow.shared.ProvenanceEvent;
import zincflow.ui.FleetService;
import zincflow.ui.UiRoutes;

import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Handlers for the /lineage inbox and the per-FlowFile detail page.
///
/// /lineage          — full page: recent FAILED events, HTMX-polled.
/// /lineage/list     — partial refreshed by the inbox.
/// /lineage/{id}     — full page: every event for one FlowFile.
/// /lineage/{id}/events — partial refreshed by the detail timeline.
///
/// Records don't play nicely with Pebble's default attribute
/// resolver (which wants bean-style getters), so the controller maps
/// {@link ProvenanceEvent} values onto plain Maps before render —
/// same pattern as {@link FlowController}.
public final class LineageController {

    private final FleetService fleet;
    private final PebbleEngine pebble;

    public LineageController(FleetService fleet, PebbleEngine pebble) {
        this.fleet = fleet;
        this.pebble = pebble;
    }

    public void handleLineage(Context ctx) throws Exception {
        ctx.contentType("text/html; charset=utf-8").result(render("lineage.peb", inboxModel()));
    }

    public void handleLineageList(Context ctx) throws Exception {
        ctx.contentType("text/html; charset=utf-8").result(render("lineage-list.peb", inboxModel()));
    }

    public void handleLineageDetail(Context ctx) throws Exception {
        ctx.contentType("text/html; charset=utf-8").result(render("lineage-detail.peb", detailModel(ctx)));
    }

    public void handleLineageDetailEvents(Context ctx) throws Exception {
        ctx.contentType("text/html; charset=utf-8").result(render("lineage-detail-events.peb", detailModel(ctx)));
    }

    private Map<String, Object> inboxModel() {
        Map<String, Object> model = new LinkedHashMap<>();
        model.put("workerUrl", fleet.workerBaseUrl().toString());
        model.put("lineageListUrl", UiRoutes.LINEAGE_LIST);
        try {
            List<ProvenanceEvent> recent = fleet.failures();
            model.put("failures", recent.stream().map(LineageController::eventToMap).toList());
        } catch (RuntimeException ex) {
            model.put("lineageError", ex.getMessage());
        }
        return model;
    }

    private Map<String, Object> detailModel(Context ctx) {
        Map<String, Object> model = new LinkedHashMap<>();
        model.put("workerUrl", fleet.workerBaseUrl().toString());
        String idParam = ctx.pathParam("id");
        model.put("flowFileId", idParam);
        long id;
        try {
            id = Long.parseLong(idParam);
        } catch (NumberFormatException ex) {
            model.put("lineageError", "FlowFile id " + idParam + " is not a number");
            model.put("events", List.of());
            model.put("detailEventsUrl", UiRoutes.LINEAGE.concat("/").concat(idParam).concat("/events"));
            return model;
        }
        model.put("detailEventsUrl", UiRoutes.LINEAGE.concat("/").concat(idParam).concat("/events"));
        try {
            List<ProvenanceEvent> events = fleet.lineage(id);
            model.put("events", events.stream().map(LineageController::eventToMap).toList());
        } catch (RuntimeException ex) {
            model.put("lineageError", ex.getMessage());
            model.put("events", List.of());
        }
        return model;
    }

    private static Map<String, Object> eventToMap(ProvenanceEvent e) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("flowFileId",      e.flowFileId());
        out.put("type",            e.type());
        out.put("component",       e.component());
        out.put("details",         e.details());
        out.put("timestampMillis", e.timestampMillis());
        return out;
    }

    private String render(String template, Map<String, Object> model) throws Exception {
        PebbleTemplate t = pebble.getTemplate("templates/" + template);
        StringWriter sw = new StringWriter();
        t.evaluate(sw, model);
        return sw.toString();
    }
}
