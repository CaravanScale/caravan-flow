package zincflow.ui;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class LineageViewTest {

    private FakeWorker worker;
    private UiMain ui;
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void teardown() {
        if (ui != null) ui.stop();
        if (worker != null) worker.stop();
    }

    @Test
    void inboxRendersRecentFailures() throws Exception {
        worker = new FakeWorker()
                .withIdentity(FakeWorker.sampleIdentity())
                .withFailures(List.of(
                        FakeWorker.event(42, "FAILED", "parser",  "bad json", 1000L),
                        FakeWorker.event(43, "FAILED", "enricher","timeout",  2000L)))
                .start();
        ui = new UiMain(worker.url()).start(0);

        String html = get("/lineage").body();
        assertTrue(html.contains("ff-42"),  "ff-42 must appear in inbox");
        assertTrue(html.contains("ff-43"),  "ff-43 must appear in inbox");
        assertTrue(html.contains("bad json"));
        assertTrue(html.contains("enricher"));
        assertTrue(html.contains("hx-get=\"/lineage/list\""),
                "inbox must poll /lineage/list");
        assertTrue(html.contains("/lineage/42"), "row must link to detail page");
    }

    @Test
    void inboxEmptyStateWhenNoFailures() throws Exception {
        worker = new FakeWorker()
                .withIdentity(FakeWorker.sampleIdentity())
                .withFailures(List.of())
                .start();
        ui = new UiMain(worker.url()).start(0);

        String html = get("/lineage").body();
        assertTrue(html.contains("No failures"), "empty state must render");
    }

    @Test
    void inboxPartialHasNoLayoutChrome() throws Exception {
        worker = new FakeWorker()
                .withIdentity(FakeWorker.sampleIdentity())
                .withFailures(List.of(
                        FakeWorker.event(7, "FAILED", "p", "oops", 100L)))
                .start();
        ui = new UiMain(worker.url()).start(0);

        String html = get("/lineage/list").body();
        assertFalse(html.contains("<nav>"), "partial must skip the page layout");
        assertTrue(html.contains("ff-7"));
    }

    @Test
    void detailPageRendersEventTimeline() throws Exception {
        worker = new FakeWorker()
                .withIdentity(FakeWorker.sampleIdentity())
                .withLineage(42, List.of(
                        FakeWorker.event(42, "CREATED",   "ingress",  "",          1000L),
                        FakeWorker.event(42, "PROCESSED", "stage",    "",          1500L),
                        FakeWorker.event(42, "FAILED",    "terminal", "kaboom",    2000L)))
                .start();
        ui = new UiMain(worker.url()).start(0);

        String html = get("/lineage/42").body();
        assertTrue(html.contains("ff-42"));
        assertTrue(html.contains("CREATED"));
        assertTrue(html.contains("PROCESSED"));
        assertTrue(html.contains("FAILED"));
        assertTrue(html.contains("kaboom"));
        assertTrue(html.contains("hx-get=\"/lineage/42/events\""),
                "detail page must poll /lineage/{id}/events");
    }

    @Test
    void detailEmptyStateWhenNoEvents() throws Exception {
        worker = new FakeWorker()
                .withIdentity(FakeWorker.sampleIdentity())
                .withLineage(99, List.of())
                .start();
        ui = new UiMain(worker.url()).start(0);

        String html = get("/lineage/99").body();
        assertTrue(html.contains("No events recorded for ff-99"));
    }

    @Test
    void detailRejectsNonNumericId() throws Exception {
        worker = new FakeWorker()
                .withIdentity(FakeWorker.sampleIdentity())
                .start();
        ui = new UiMain(worker.url()).start(0);

        String html = get("/lineage/abc").body();
        // Non-numeric id shouldn't reach the worker — UI renders a
        // client-side error surface instead.
        assertTrue(html.contains("abc is not a number"));
    }

    @Test
    void detailSurfacesWorkerUnreachable() throws Exception {
        // No /api/provenance/42 registered → worker 404s → UI
        // renders the error banner rather than blanking out.
        worker = new FakeWorker().withIdentity(FakeWorker.sampleIdentity()).start();
        ui = new UiMain(worker.url()).start(0);

        String html = get("/lineage/42").body();
        assertTrue(html.contains("Worker unreachable"));
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + ui.port() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
