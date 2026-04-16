package zincflow.processors;

import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.ProcessorResult;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class ProcessorsTest {

    // --- UpdateAttribute ---

    @Test
    void updateAttributeSetsKeyAndWrapsInSingle() {
        var ff = FlowFile.create(new byte[0], Map.of());
        var result = new UpdateAttribute("priority", "high").process(ff);
        assertInstanceOf(ProcessorResult.Single.class, result);
        var single = (ProcessorResult.Single) result;
        assertEquals("high", single.flowFile().attributes().get("priority"));
    }

    @Test
    void updateAttributeBlankKeyRejected() {
        assertThrows(IllegalArgumentException.class, () -> new UpdateAttribute("", "v"));
        assertThrows(IllegalArgumentException.class, () -> new UpdateAttribute(null, "v"));
    }

    // --- LogAttribute ---

    @Test
    void logAttributePassesThroughUnchanged() {
        var ff = FlowFile.create(new byte[]{1, 2, 3}, Map.of("k", "v"));
        var result = new LogAttribute("[t]").process(ff);
        assertInstanceOf(ProcessorResult.Single.class, result);
        assertSame(ff, ((ProcessorResult.Single) result).flowFile(),
                "LogAttribute must be a pass-through — same FlowFile reference");
    }

    // --- RouteOnAttribute ---

    @Test
    void routeOnAttributeMatchesFirstRule() {
        var proc = new RouteOnAttribute("high: priority == urgent; low: priority == normal");
        var ff = FlowFile.create(new byte[0], Map.of("priority", "urgent"));
        var result = proc.process(ff);
        assertInstanceOf(ProcessorResult.Routed.class, result);
        assertEquals("high", ((ProcessorResult.Routed) result).route());
    }

    @Test
    void routeOnAttributeMatchesSecondRule() {
        var proc = new RouteOnAttribute("high: priority == urgent; low: priority == normal");
        var ff = FlowFile.create(new byte[0], Map.of("priority", "normal"));
        var result = proc.process(ff);
        assertEquals("low", ((ProcessorResult.Routed) result).route());
    }

    @Test
    void routeOnAttributeFallsBackToUnmatched() {
        var proc = new RouteOnAttribute("high: priority == urgent");
        var ff = FlowFile.create(new byte[0], Map.of("priority", "bogus"));
        var result = proc.process(ff);
        assertEquals("unmatched", ((ProcessorResult.Routed) result).route());
    }

    @Test
    void routeOnAttributeNeqOperator() {
        var proc = new RouteOnAttribute("errors: status != ok");
        var okFf = FlowFile.create(new byte[0], Map.of("status", "ok"));
        var badFf = FlowFile.create(new byte[0], Map.of("status", "fail"));
        assertEquals("unmatched", ((ProcessorResult.Routed) proc.process(okFf)).route());
        assertEquals("errors", ((ProcessorResult.Routed) proc.process(badFf)).route());
    }

    @Test
    void routeOnAttributeBlankSpecYieldsUnmatched() {
        var proc = new RouteOnAttribute("");
        var ff = FlowFile.create(new byte[0], Map.of("x", "y"));
        assertEquals("unmatched", ((ProcessorResult.Routed) proc.process(ff)).route());
    }

    @Test
    void routeOnAttributeMalformedRuleRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new RouteOnAttribute("missingcolon"));
        assertThrows(IllegalArgumentException.class,
                () -> new RouteOnAttribute("r: attr_only"));
        assertThrows(IllegalArgumentException.class,
                () -> new RouteOnAttribute("r: attr BADOP value"));
    }
}
