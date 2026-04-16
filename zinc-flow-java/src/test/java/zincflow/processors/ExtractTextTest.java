package zincflow.processors;

import org.junit.jupiter.api.Test;
import zincflow.core.ClaimContent;
import zincflow.core.FlowFile;
import zincflow.core.MemoryContentStore;
import zincflow.core.ProcessorResult;
import zincflow.core.RecordContent;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class ExtractTextTest {

    @Test
    void namedGroupsBecomeAttributes() {
        var proc = new ExtractText(
                "order=(?<orderId>\\d+) user=(?<user>\\w+)",
                "",
                null);
        var ff = FlowFile.create("order=42 user=alice".getBytes(StandardCharsets.UTF_8), Map.of());
        var result = proc.process(ff);
        assertInstanceOf(ProcessorResult.Single.class, result);
        var out = ((ProcessorResult.Single) result).flowFile();
        assertEquals("42", out.attributes().get("orderId"));
        assertEquals("alice", out.attributes().get("user"));
    }

    @Test
    void positionalGroupsMappedViaGroupNames() {
        var proc = new ExtractText("(\\w+)@(\\w+\\.\\w+)", "localpart, domain", null);
        var ff = FlowFile.create("contact: bob@example.com".getBytes(StandardCharsets.UTF_8), Map.of());
        var result = proc.process(ff);
        var out = ((ProcessorResult.Single) result).flowFile();
        assertEquals("bob", out.attributes().get("localpart"));
        assertEquals("example.com", out.attributes().get("domain"));
    }

    @Test
    void noMatchPassesFlowFileThroughUnchanged() {
        var proc = new ExtractText("NOMATCH-(\\d+)", "", null);
        var ff = FlowFile.create("payload".getBytes(StandardCharsets.UTF_8),
                Map.of("preserved", "yes"));
        var result = proc.process(ff);
        var out = ((ProcessorResult.Single) result).flowFile();
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
        var result = proc.process(ff);
        var out = ((ProcessorResult.Single) result).flowFile();
        assertEquals("7", out.attributes().get("x"));
    }

    @Test
    void recordContentFails() {
        var proc = new ExtractText("x=(?<x>\\d+)", "", null);
        var ff = FlowFile.create(new RecordContent(List.of(Map.of("x", 7))), Map.of());
        var result = proc.process(ff);
        assertInstanceOf(ProcessorResult.Failure.class, result);
    }

    @Test
    void blankPatternRejectedAtConstruction() {
        assertThrows(IllegalArgumentException.class, () -> new ExtractText("", "", null));
        assertThrows(IllegalArgumentException.class, () -> new ExtractText(null, "", null));
    }
}
