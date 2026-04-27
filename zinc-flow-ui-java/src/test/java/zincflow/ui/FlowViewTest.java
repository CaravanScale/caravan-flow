package zincflow.ui;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

final class FlowViewTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private FakeWorker worker;
    private UiMain ui;
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void teardown() {
        if (ui != null) ui.stop();
        if (worker != null) worker.stop();
    }

    @Test
    void flowPageShipsGraphJsonAndCytoscapeBootstrap() throws Exception {
        boot();
        String html = get("/flow").body();
        assertTrue(html.contains("<div id=\"flow-graph\""),   "cytoscape mount point");
        assertTrue(html.contains("<aside id=\"flow-drawer\""), "drawer container");
        assertTrue(html.contains("<script src=\"/static/cytoscape.min.js\""),
                "cytoscape script tag");
        assertTrue(html.contains("<script src=\"/static/flow.js\""),
                "flow.js bootstrap");

        // The embedded JSON blob must carry both processors (as nodes)
        // and the connection flattened to source/target edges.
        String blob = extractDataBlob(html);
        JsonNode data = JSON.readTree(blob);
        assertEquals(2, data.get("processors").size());
        assertEquals("ingress", data.get("processors").get(0).get("name").asText());
        assertTrue(data.get("edges").size() >= 1,
                "connection ingress→tail should surface as at least one edge");
        JsonNode edge = data.get("edges").get(0);
        assertEquals("ingress", edge.get("source").asText());
        assertEquals("tail",    edge.get("target").asText());
        assertEquals("success", edge.get("label").asText());
    }

    @Test
    void flowStatsEndpointReturnsLiveProcessorState() throws Exception {
        boot();
        HttpResponse<String> resp = get("/flow/stats.json");
        assertEquals(200, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertTrue(body.has("processors"));
        assertEquals("ingress", body.get("processors").get(0).get("name").asText());
        assertEquals("ENABLED", body.get("processors").get(0).get("state").asText());
    }

    @Test
    void flowPanelRendersProcessorDetail() throws Exception {
        boot();
        String html = get("/flow/panel/ingress").body();
        assertTrue(html.contains("ingress"),      "processor name");
        assertTrue(html.contains("LogAttribute"), "processor type");
        assertTrue(html.contains("ENABLED"),      "state badge");
        assertTrue(html.contains("hx-get=\"/flow/panel/ingress\""),
                "panel must self-refresh via HTMX");
        assertTrue(html.contains("hx-trigger=\"every 2s\""),
                "panel must poll every 2s");
        // Config table present (sample flow wires ingress without
        // config, so the empty-state copy should render).
        assertTrue(html.contains("Configuration"));
    }

    @Test
    void flowPanelRendersConfigWhenPresent() throws Exception {
        worker = new FakeWorker()
                .withIdentity(FakeWorker.sampleIdentity())
                .withFlow(FakeWorker.sampleFlowWithConfig())
                .start();
        ui = new UiMain(worker.url()).start(0);
        String html = get("/flow/panel/ingress").body();
        assertTrue(html.contains("prefix"), "config key rendered");
        assertTrue(html.contains("[in] "),  "config value rendered");
    }

    @Test
    void flowPanelForUnknownProcessorRendersError() throws Exception {
        boot();
        String html = get("/flow/panel/does-not-exist").body();
        assertTrue(html.contains("processor not found"));
    }

    @Test
    void flowPageSurfacesWorkerUnreachable() throws Exception {
        ui = new UiMain(URI.create("http://localhost:1")).start(0);
        String html = get("/flow").body();
        // Both user-visible banner and machine-readable JSON carry the
        // error so the client script can render it.
        assertTrue(html.contains("class=\"flow-error\""),
                "error placeholder must render");
        String blob = extractDataBlob(html);
        JsonNode data = JSON.readTree(blob);
        assertTrue(data.has("error"), "graph-data JSON must carry the error");
    }

    // --- helpers ---

    private void boot() {
        worker = new FakeWorker()
                .withIdentity(FakeWorker.sampleIdentity())
                .withFlow(FakeWorker.sampleFlow())
                .start();
        ui = new UiMain(worker.url()).start(0);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + ui.port() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /// Pull the JSON inside &lt;script id="flow-data"&gt;…&lt;/script&gt;.
    private static String extractDataBlob(String html) {
        int open = html.indexOf("id=\"flow-data\">");
        assertTrue(open > 0, "flow-data script block must exist");
        int contentStart = html.indexOf('>', open) + 1;
        int contentEnd = html.indexOf("</script>", contentStart);
        return html.substring(contentStart, contentEnd);
    }
}
