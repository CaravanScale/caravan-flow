package zincflow.shared;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/// Wire-format sanity: every DTO in this module must parse real
/// zinc-flow-java responses and emit the same shape back.
final class DtoRoundTripTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void identityRoundTrip() throws Exception {
        String wire = """
                {"nodeId":"abc","hostname":"host","version":"1.0",
                 "port":9092,"uptimeMillis":100,"bootMillis":200}""";
        Identity parsed = JSON.readValue(wire, Identity.class);
        assertEquals("abc",  parsed.nodeId());
        assertEquals("host", parsed.hostname());
        assertEquals(9092,   parsed.port());
        assertEquals(100L,   parsed.uptimeMillis());
    }

    @Test
    void identityToleratesUnknownFields() throws Exception {
        Identity parsed = JSON.readValue("""
                {"nodeId":"abc","hostname":"h","version":"v","port":1,
                 "uptimeMillis":0,"bootMillis":0,"futureField":"ignored"}""", Identity.class);
        assertEquals("abc", parsed.nodeId());
    }

    @Test
    void flowSnapshotRoundTrip() throws Exception {
        String wire = """
                {"entryPoints":["ingress"],
                 "processors":[
                   {"name":"ingress","type":"LogAttribute","state":"ENABLED",
                    "stats":{"processed":5},
                    "connections":{"success":["tail"]}}],
                 "connections":{"ingress":{"success":["tail"]}},
                 "providers":[{"name":"logging","type":"LoggingProvider","state":"ENABLED"}],
                 "sources":[{"name":"gen","type":"GenerateFlowFile","running":true}],
                 "stats":{"processed":5,"dropped":0}}""";
        FlowSnapshot snap = JSON.readValue(wire, FlowSnapshot.class);
        assertEquals(List.of("ingress"), snap.entryPoints());
        assertEquals(1, snap.processors().size());
        assertEquals("LogAttribute", snap.processors().get(0).type());
        assertEquals(5L, snap.processors().get(0).stats().get("processed"));
        assertEquals(List.of("tail"),
                snap.connections().get("ingress").get("success"));
        assertEquals("logging", snap.providers().get(0).name());
        assertTrue(snap.sources().get(0).running());
    }

    @Test
    void provenanceEventRoundTrip() throws Exception {
        String wire = """
                {"flowFileId":42,"type":"FAILED","component":"parser",
                 "details":"bad json","timestampMillis":123}""";
        ProvenanceEvent e = JSON.readValue(wire, ProvenanceEvent.class);
        assertEquals(42L, e.flowFileId());
        assertEquals("FAILED", e.type());
        assertEquals("parser", e.component());
    }

    @Test
    void nodeEntryRoundTrip() throws Exception {
        NodeEntry written = new NodeEntry("n1", "http://w1:9092", "host-1", "1.0", 1000L, "self");
        String wire = JSON.writeValueAsString(written);
        NodeEntry back = JSON.readValue(wire, NodeEntry.class);
        assertEquals(written, back);
    }

    @Test
    void flowSnapshotStatsAcceptsNestedObjects() throws Exception {
        // The worker's /api/flow emits Map<String, Object> stats where
        // some entries are numbers and others are nested maps
        // (e.g. processorCounts). Long-typed stats would break on the
        // nested entry — keeping it Object.
        FlowSnapshot snap = JSON.readValue("""
                {"entryPoints":[],"processors":[],"connections":{},
                 "providers":[],"sources":[],
                 "stats":{"processed":10,"processorCounts":{"a":5}}}""",
                FlowSnapshot.class);
        assertEquals(10, ((Number) snap.stats().get("processed")).intValue());
        assertTrue(snap.stats().get("processorCounts") instanceof Map);
    }

    @Test
    void flowSnapshotEmptyBuilder() {
        FlowSnapshot empty = FlowSnapshot.empty();
        assertTrue(empty.entryPoints().isEmpty());
        assertTrue(empty.processors().isEmpty());
        assertTrue(empty.connections().isEmpty());
        assertTrue(empty.providers().isEmpty());
        assertTrue(empty.sources().isEmpty());
        assertTrue(empty.stats().isEmpty());
    }

    @Test
    void routeNamesAreConstants() {
        assertEquals("/api/identity", RouteNames.API_IDENTITY);
        assertEquals("/api/flow",     RouteNames.API_FLOW);
        assertEquals("/api/provenance/", RouteNames.API_PROVENANCE_BY_ID);
        assertTrue(RouteNames.API_PROVENANCE_BY_ID.endsWith("/"));
    }
}
