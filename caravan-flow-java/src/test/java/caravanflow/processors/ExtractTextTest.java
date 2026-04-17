package caravanflow.processors;

import org.junit.jupiter.api.Test;
import caravanflow.core.ClaimContent;
import caravanflow.core.FlowFile;
import caravanflow.core.MemoryContentStore;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RecordContent;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class ExtractTextTest {

    private static FlowFile single(ProcessorResult result) {
        return switch (result) {
            case ProcessorResult.Single(FlowFile ff) -> ff;
            default -> {
                fail("expected Single, got " + result);
                yield null;
            }
        };
    }

    @Test
    void namedGroupsBecomeAttributes() {
        var proc = new ExtractText(
                "order=(?<orderId>\\d+) user=(?<user>\\w+)",
                "",
                null);
        var ff = FlowFile.create("order=42 user=alice".getBytes(StandardCharsets.UTF_8), Map.of());
        var out = single(proc.process(ff));
        assertEquals("42", out.attributes().get("orderId"));
        assertEquals("alice", out.attributes().get("user"));
    }

    @Test
    void positionalGroupsMappedViaGroupNames() {
        var proc = new ExtractText("(\\w+)@(\\w+\\.\\w+)", "localpart, domain", null);
        var ff = FlowFile.create("contact: bob@example.com".getBytes(StandardCharsets.UTF_8), Map.of());
        var out = single(proc.process(ff));
        assertEquals("bob", out.attributes().get("localpart"));
        assertEquals("example.com", out.attributes().get("domain"));
    }

    @Test
    void noMatchPassesFlowFileThroughUnchanged() {
        var proc = new ExtractText("NOMATCH-(\\d+)", "", null);
        var ff = FlowFile.create("payload".getBytes(StandardCharsets.UTF_8),
                Map.of("preserved", "yes"));
        var out = single(proc.process(ff));
        assertSame(ff, out, "no-match branch should pass the original FlowFile through unchanged");
        assertEquals("yes", out.attributes().get("preserved"));
    }

    @Test
    void resolvesClaimContentViaStore() {
        var store = new MemoryContentStore();
        byte[] payload = "x=7".getBytes(StandardCharsets.UTF_8);
        String claimId = store.store(payload);
        var proc = new ExtractText("x=(?<x>\\d+)", "", store);
        var ff = FlowFile.create(new ClaimContent(claimId, payload.length), Map.of());
        var out = single(proc.process(ff));
        assertEquals("7", out.attributes().get("x"));
    }

    @Test
    void recordContentFails() {
        var proc = new ExtractText("x=(?<x>\\d+)", "", null);
        var ff = FlowFile.create(new RecordContent(List.of(Map.of("x", 7))), Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, proc.process(ff));
    }

    @Test
    void blankPatternRejectedAtConstruction() {
        assertThrows(IllegalArgumentException.class, () -> new ExtractText("", "", null));
        assertThrows(IllegalArgumentException.class, () -> new ExtractText(null, "", null));
    }
}
