package zincflow.ui;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

final class SettingsViewTest {

    private FakeWorker worker;
    private UiMain ui;
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void teardown() {
        if (ui != null) ui.stop();
        if (worker != null) worker.stop();
    }

    @Test
    void settingsPageRendersOverlayStack() throws Exception {
        worker = new FakeWorker()
                .withIdentity(FakeWorker.sampleIdentity())
                .withOverlays(FakeWorker.sampleOverlays())
                .start();
        ui = new UiMain(worker.url()).start(0);

        String html = get("/settings").body();
        // Each layer row
        assertTrue(html.contains("base"),                       "base layer row");
        assertTrue(html.contains("local"),                      "local layer row");
        assertTrue(html.contains("secrets"),                    "secrets layer row");
        assertTrue(html.contains("/etc/zinc/config.yaml"),   "base path");
        // Missing layer is marked
        assertTrue(html.contains("layer-missing"), "missing layer must render the dimmed style");
        // Provenance table entries
        assertTrue(html.contains("flow.entryPoints"));
        assertTrue(html.contains("http.port"));
    }

    @Test
    void settingsSurfacesWorkerUnreachable() throws Exception {
        // No /api/overlays registered → worker 404s → UI shows error.
        worker = new FakeWorker().withIdentity(FakeWorker.sampleIdentity()).start();
        ui = new UiMain(worker.url()).start(0);
        String html = get("/settings").body();
        assertTrue(html.contains("Worker unreachable"));
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + ui.port() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
