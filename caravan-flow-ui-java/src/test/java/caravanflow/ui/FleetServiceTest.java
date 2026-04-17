package caravanflow.ui;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import caravanflow.shared.Identity;
import caravanflow.shared.ProvenanceEvent;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class FleetServiceTest {

    private FakeWorker worker;

    @AfterEach
    void teardown() { if (worker != null) worker.stop(); }

    @Test
    void identityFetchesAndCaches() {
        worker = new FakeWorker().withIdentity(FakeWorker.sampleIdentity()).start();
        var fleet = new FleetService(worker.url(), Duration.ofSeconds(2));

        Identity first = fleet.identity();
        Identity second = fleet.identity();

        assertEquals("node-test", first.nodeId());
        assertEquals(9092, first.port());
        assertSame(first, second, "second call should hit cache, not worker");
        assertEquals(1, worker.identityHits(), "worker should only see one call");
    }

    @Test
    void workerReachableFalseWhenIdentityFails() {
        worker = new FakeWorker().start(); // no /api/identity handler — 404
        var fleet = new FleetService(worker.url(), Duration.ofSeconds(1));
        assertFalse(fleet.workerReachable());
    }

    @Test
    void workerReachableTrueOnHappyPath() {
        worker = new FakeWorker().withIdentity(FakeWorker.sampleIdentity()).start();
        var fleet = new FleetService(worker.url(), Duration.ofSeconds(1));
        assertTrue(fleet.workerReachable());
    }

    @Test
    void rejectsNullBaseUrl() {
        assertThrows(IllegalArgumentException.class, () -> new FleetService(null));
    }

    @Test
    void unreachableWorkerThrowsFromIdentity() {
        // port 1 is almost certainly not listening — connect refused.
        var fleet = new FleetService(URI.create("http://localhost:1"), Duration.ofMillis(300));
        assertThrows(RuntimeException.class, fleet::identity);
    }

    @Test
    void failuresParsesProvenanceEventList() {
        worker = new FakeWorker()
                .withFailures(List.of(
                        FakeWorker.event(42, "FAILED", "parser",  "bad json", 1000L),
                        FakeWorker.event(43, "FAILED", "enricher","timeout",  2000L)))
                .start();
        var fleet = new FleetService(worker.url(), Duration.ofSeconds(2));

        List<ProvenanceEvent> out = fleet.failures();
        assertEquals(2, out.size());
        assertEquals(42L,       out.get(0).flowFileId());
        assertEquals("FAILED",  out.get(0).type());
        assertEquals("parser",  out.get(0).component());
        assertEquals("bad json",out.get(0).details());
        assertEquals(1000L,     out.get(0).timestampMillis());
    }

    @Test
    void lineageParsesProvenanceEventList() {
        worker = new FakeWorker()
                .withLineage(42, List.of(
                        FakeWorker.event(42, "CREATED",   "ingress", "",       1000L),
                        FakeWorker.event(42, "PROCESSED", "stage",   "",       1500L)))
                .start();
        var fleet = new FleetService(worker.url(), Duration.ofSeconds(2));

        List<ProvenanceEvent> out = fleet.lineage(42);
        assertEquals(2, out.size());
        assertEquals("CREATED",   out.get(0).type());
        assertEquals("PROCESSED", out.get(1).type());
    }

    @Test
    void failuresRejectsNonPositiveN() {
        var fleet = new FleetService(URI.create("http://localhost:1"), Duration.ofMillis(300));
        assertThrows(IllegalArgumentException.class, () -> fleet.failures(0));
        assertThrows(IllegalArgumentException.class, () -> fleet.failures(-1));
    }
}
