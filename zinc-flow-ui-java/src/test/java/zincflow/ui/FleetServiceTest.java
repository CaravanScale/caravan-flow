package zincflow.ui;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zincflow.shared.Identity;

import java.net.URI;
import java.time.Duration;

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
}
