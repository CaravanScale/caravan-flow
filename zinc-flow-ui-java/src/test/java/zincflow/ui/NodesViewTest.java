package zincflow.ui;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

final class NodesViewTest {

    private FakeWorker worker;
    private UiMain ui;
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void teardown() {
        if (ui != null) ui.stop();
        if (worker != null) worker.stop();
    }

    @Test
    void nodesPageShowsSingleWorkerIdentity() throws Exception {
        worker = new FakeWorker().withIdentity(FakeWorker.sampleIdentity()).start();
        ui = new UiMain(worker.url()).start(0);

        String html = get("/nodes").body();
        assertTrue(html.contains("Phase 1 — single-worker mode"),
                "placeholder banner must render");
        assertTrue(html.contains("node-test"), "nodeId from identity");
        assertTrue(html.contains("unit-test"), "hostname from identity");
        assertTrue(html.contains("9.9.9"),     "version from identity");
    }

    @Test
    void nodesSurfacesWorkerUnreachable() throws Exception {
        ui = new UiMain(URI.create("http://localhost:1")).start(0);
        String html = get("/nodes").body();
        assertTrue(html.contains("Worker unreachable"));
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + ui.port() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
