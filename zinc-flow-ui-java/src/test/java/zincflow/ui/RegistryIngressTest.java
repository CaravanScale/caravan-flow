package zincflow.ui;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zincflow.shared.NodeEntry;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class RegistryIngressTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private UiMain ui;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void boot() {
        // No worker needed — the UI's health checks don't run during
        // ingress tests, so pointing at localhost:1 (closed) is safe.
        ui = new UiMain(URI.create("http://localhost:1")).start(0);
    }

    @AfterEach
    void teardown() { if (ui != null) ui.stop(); }

    @Test
    void registerAcceptsIdentityPayloadAndStoresEntry() throws Exception {
        HttpResponse<String> resp = post("/api/registry/register", Map.of(
                "nodeId",       "worker-7",
                "hostname",     "w7.cluster.local",
                "version",      "1.2.3",
                "port",         9092,
                "uptimeMillis", 12345L,
                "bootMillis",   1L));
        assertEquals(201, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals("registered", body.get("status").asText());
        assertEquals("worker-7",   body.get("nodeId").asText());

        NodeEntry entry = ui.registry().get("worker-7");
        assertNotNull(entry);
        assertEquals("worker-7",         entry.nodeId());
        assertEquals("w7.cluster.local", entry.hostname());
        assertEquals("1.2.3",            entry.version());
        assertEquals("self",             entry.source());
        assertTrue (entry.url() != null && entry.url().contains(":9092"),
                "reconstructed URL should include the worker port");
        assertTrue (entry.lastHeartbeatMillis() > 0);
    }

    @Test
    void heartbeatRefreshesTimestampOnExistingEntry() throws Exception {
        post("/api/registry/register", Map.of(
                "nodeId",   "worker-7", "hostname", "w7",
                "version",  "1.0",     "port",      9092));
        long firstTs = ui.registry().get("worker-7").lastHeartbeatMillis();

        Thread.sleep(5);
        HttpResponse<String> resp = post("/api/registry/heartbeat", Map.of(
                "nodeId",   "worker-7", "hostname", "w7",
                "version",  "1.0",     "port",      9092));
        assertEquals(200, resp.statusCode());

        NodeEntry entry = ui.registry().get("worker-7");
        assertTrue(entry.lastHeartbeatMillis() >= firstTs,
                "heartbeat should not regress the timestamp");
    }

    @Test
    void registerRejectsEmptyBody() throws Exception {
        HttpResponse<String> resp = rawPost("/api/registry/register", new byte[0]);
        assertEquals(400, resp.statusCode());
        assertEquals(0, ui.registry().size());
    }

    @Test
    void registerRejectsMissingNodeId() throws Exception {
        HttpResponse<String> resp = post("/api/registry/register", Map.of(
                "hostname", "w7", "version", "1.0", "port", 9092));
        assertEquals(400, resp.statusCode());
        assertEquals(0, ui.registry().size());
    }

    @Test
    void registerRejectsMalformedJson() throws Exception {
        HttpResponse<String> resp = rawPost("/api/registry/register", "not json".getBytes());
        assertEquals(400, resp.statusCode());
        assertEquals(0, ui.registry().size());
    }

    @Test
    void listReturnsRegisteredEntries() throws Exception {
        post("/api/registry/register", Map.of(
                "nodeId", "a", "hostname", "ha", "version", "1", "port", 9001));
        post("/api/registry/register", Map.of(
                "nodeId", "b", "hostname", "hb", "version", "1", "port", 9002));

        HttpResponse<String> resp = get("/api/registry/nodes");
        assertEquals(200, resp.statusCode());
        List<NodeEntry> nodes = JSON.readValue(resp.body(),
                JSON.getTypeFactory().constructCollectionType(List.class, NodeEntry.class));
        assertEquals(2, nodes.size());
    }

    @Test
    void registerIsIdempotentPerNodeId() throws Exception {
        post("/api/registry/register", Map.of(
                "nodeId", "a", "hostname", "first",  "version", "1", "port", 9001));
        post("/api/registry/register", Map.of(
                "nodeId", "a", "hostname", "second", "version", "2", "port", 9002));
        assertEquals(1, ui.registry().size());
        NodeEntry entry = ui.registry().get("a");
        assertEquals("second", entry.hostname(),
                "second register for same nodeId must overwrite, not append");
        assertEquals("2", entry.version());
    }

    private HttpResponse<String> post(String path, Map<String, Object> body) throws Exception {
        return rawPost(path, JSON.writeValueAsBytes(body));
    }

    private HttpResponse<String> rawPost(String path, byte[] body) throws Exception {
        return http.send(HttpRequest.newBuilder(url(path))
                        .header("content-type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(url(path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private URI url(String path) {
        return URI.create("http://localhost:" + ui.port() + path);
    }
}
