package caravanflow.ui;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

final class FlowViewTest {

    private FakeWorker worker;
    private UiMain ui;
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void teardown() {
        if (ui != null) ui.stop();
        if (worker != null) worker.stop();
    }

    @Test
    void flowPageRendersProcessorsInColumns() throws Exception {
        boot();
        String html = get("/flow").body();
        // both processors present
        assertTrue(html.contains("ingress"), "ingress card must render");
        assertTrue(html.contains("tail"),    "tail card must render");
        // HTMX polling hook
        assertTrue(html.contains("hx-get=\"/flow/cards\""),
                "polling trigger must be present");
        assertTrue(html.contains("hx-trigger=\"every 2s\""),
                "2s refresh interval must be present");
        // state badge + stats
        assertTrue(html.contains("ENABLED"));
        assertTrue(html.contains("processed"));
    }

    @Test
    void flowCardsPartialReturnsJustTheGrid() throws Exception {
        boot();
        String html = get("/flow/cards").body();
        // partial: no layout nav
        assertFalse(html.contains("<nav>"));
        // but still has the column grid + cards
        assertTrue(html.contains("class=\"columns\""));
        assertTrue(html.contains("ingress"));
    }

    @Test
    void flowPageSurfacesWorkerUnreachable() throws Exception {
        ui = new UiMain(URI.create("http://localhost:1")).start(0);
        String html = get("/flow").body();
        assertTrue(html.contains("Worker unreachable"));
    }

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
}
