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

final class UiMainTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private FakeWorker worker;
    private UiMain ui;
    private final HttpClient http = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();

    @AfterEach
    void teardown() {
        if (ui != null) ui.stop();
        if (worker != null) worker.stop();
    }

    @Test
    void rootRedirectsToFlow() throws Exception {
        bootWithFakeWorker();
        var resp = get("/");
        assertEquals(302, resp.statusCode());
        assertTrue(resp.headers().firstValue("location").orElse("").endsWith("/flow"));
    }

    @Test
    void healthReportsWorkerReachable() throws Exception {
        bootWithFakeWorker();
        var resp = get("/health");
        assertEquals(200, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertEquals("healthy", body.get("status").asText());
        assertTrue(body.get("workerReachable").asBoolean());
        assertEquals(worker.url().toString(), body.get("workerUrl").asText());
    }

    @Test
    void healthReportsWorkerUnreachable() throws Exception {
        // Point UI at a port that nothing listens on.
        ui = new UiMain(URI.create("http://localhost:1")).start(0);
        var resp = get("/health");
        assertEquals(200, resp.statusCode());
        JsonNode body = JSON.readTree(resp.body());
        assertFalse(body.get("workerReachable").asBoolean());
    }

    @Test
    void flowPageServesHtml() throws Exception {
        bootWithFakeWorker();
        var resp = get("/flow");
        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains("<code>" + worker.url() + "</code>"),
                "rendered HTML must include the worker URL");
        assertTrue(resp.headers().firstValue("content-type").orElse("").startsWith("text/html"));
    }

    @Test
    void staticAssetsMounted() throws Exception {
        bootWithFakeWorker();
        var resp = get("/static/zinc-ui.css");
        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains("zinc-flow"), "static CSS must be served from classpath");
    }

    @Test
    void configResolvesCliFirst() {
        UiMain.Config c = UiMain.Config.resolve(new String[] {"--worker", "http://w:1", "--port", "7777"});
        assertEquals(URI.create("http://w:1"), c.workerUrl());
        assertEquals(7777, c.port());
    }

    @Test
    void configFallsBackToDefaults() {
        UiMain.Config c = UiMain.Config.resolve(new String[] {});
        assertEquals(URI.create(UiMain.DEFAULT_WORKER), c.workerUrl());
        assertEquals(UiMain.DEFAULT_PORT, c.port());
    }

    // --- helpers ---

    private void bootWithFakeWorker() {
        worker = new FakeWorker().withIdentity(FakeWorker.sampleIdentity()).start();
        ui = new UiMain(worker.url()).start(0);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + ui.port() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
