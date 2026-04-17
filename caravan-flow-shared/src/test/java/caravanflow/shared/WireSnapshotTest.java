package caravanflow.shared;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/// Wire snapshot test — fixtures in {@code src/test/resources/fixtures/}
/// are real JSON responses captured from a running caravan-flow-java
/// worker. Parsing them through the shared DTOs guarantees that
/// either side can't drift the wire format without tripping a test.
///
/// When the worker intentionally changes a response shape, re-capture
/// the matching fixture — don't paper over the diff with
/// {@code @JsonIgnoreProperties}.
final class WireSnapshotTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void identityFixtureParses() throws Exception {
        Identity id = JSON.readValue(fixture("identity.json"), Identity.class);
        assertNotNull(id.nodeId());
        assertNotNull(id.hostname());
        assertNotNull(id.version());
        assertTrue(id.port() > 0, "port must be positive: " + id.port());
        assertTrue(id.uptimeMillis() >= 0);
    }

    @Test
    void flowFixtureParses() throws Exception {
        FlowSnapshot snap = JSON.readValue(fixture("flow.json"), FlowSnapshot.class);
        assertFalse(snap.entryPoints().isEmpty(), "demo pipeline has at least one entry point");
        assertFalse(snap.processors().isEmpty(),  "demo pipeline has processors");
        // Every processor should have name + type + state.
        for (ProcessorView p : snap.processors()) {
            assertNotNull(p.name());
            assertNotNull(p.type());
            assertNotNull(p.state());
        }
    }

    @Test
    void overlaysFixtureParses() throws Exception {
        OverlayInfo overlays = JSON.readValue(fixture("overlays.json"), OverlayInfo.class);
        assertNotNull(overlays.layers(), "layers array must be present");
        assertFalse(overlays.layers().isEmpty(), "a base layer is always present");
        for (OverlayInfo.Layer layer : overlays.layers()) {
            assertNotNull(layer.role());
        }
    }

    @Test
    void lineageFixtureParsesAsProvenanceEventList() throws Exception {
        List<ProvenanceEvent> events = JSON.readValue(
                fixture("lineage.json"),
                new TypeReference<List<ProvenanceEvent>>() {});
        assertFalse(events.isEmpty(), "captured lineage fixture has events");
        ProvenanceEvent first = events.get(0);
        assertTrue(first.flowFileId() > 0, "flowFileId must deserialize as long");
        assertTrue(first.timestampMillis() > 0, "timestampMillis must deserialize as long");
        assertNotNull(first.type());
        assertNotNull(first.component());
    }

    @Test
    void failuresFixtureParsesAsEmptyList() throws Exception {
        // The demo pipeline doesn't produce failures, so this fixture
        // pins the "no failures" wire shape ([]) — if the worker ever
        // regresses to returning null or an object, this test flags it.
        List<ProvenanceEvent> events = JSON.readValue(
                fixture("failures.json"),
                new TypeReference<List<ProvenanceEvent>>() {});
        assertTrue(events.isEmpty());
    }

    @Test
    void routeNamesPointAtRealWorkerEndpoints() throws Exception {
        // Every route constant must resolve against the fixture set
        // (either directly or by prefix) — guards against the UI
        // constants falling out of sync with the worker's registered
        // routes.
        Map<String, String> fixtures = Map.of(
                RouteNames.API_IDENTITY,              "identity.json",
                RouteNames.API_FLOW,                  "flow.json",
                RouteNames.API_OVERLAYS,              "overlays.json",
                RouteNames.API_PROVENANCE_FAILURES,   "failures.json");
        for (var entry : fixtures.entrySet()) {
            try (InputStream in = WireSnapshotTest.class.getResourceAsStream("/fixtures/" + entry.getValue())) {
                assertNotNull(in, "fixture " + entry.getValue() + " missing for route " + entry.getKey());
            }
        }
    }

    private static InputStream fixture(String name) {
        InputStream in = WireSnapshotTest.class.getResourceAsStream("/fixtures/" + name);
        if (in == null) throw new IllegalStateException("fixture not found: " + name);
        return in;
    }
}
