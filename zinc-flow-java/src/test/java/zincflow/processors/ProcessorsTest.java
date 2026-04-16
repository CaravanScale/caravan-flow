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

    // --- FilterAttribute ---

    @Test
    void filterAttributeDropsOnMatch() {
        var proc = new FilterAttribute("drop", "yes");
        var ff = FlowFile.create(new byte[0], Map.of("drop", "yes"));
        assertSame(ProcessorResult.Dropped.INSTANCE, proc.process(ff));
    }

    @Test
    void filterAttributePassesWhenNoMatch() {
        var proc = new FilterAttribute("drop", "yes");
        var ff = FlowFile.create(new byte[0], Map.of("drop", "no"));
        assertInstanceOf(ProcessorResult.Single.class, proc.process(ff));
    }

    @Test
    void filterAttributeInvertedKeepsOnMatch() {
        var proc = new FilterAttribute("keep", "me", false); // drop when NOT matching
        var keep = FlowFile.create(new byte[0], Map.of("keep", "me"));
        var drop = FlowFile.create(new byte[0], Map.of("keep", "other"));
        assertInstanceOf(ProcessorResult.Single.class, proc.process(keep));
        assertSame(ProcessorResult.Dropped.INSTANCE, proc.process(drop));
    }

    // --- ReplaceText ---

    @Test
    void replaceTextRewritesPayload() {
        var proc = new ReplaceText("world", "Java");
        var ff = FlowFile.create("hello world".getBytes(), Map.of());
        var result = proc.process(ff);
        var out = (ProcessorResult.Single) result;
        var content = (zincflow.core.RawContent) out.flowFile().content();
        assertEquals("hello Java", new String(content.bytes()));
    }

    @Test
    void replaceTextSupportsRegexBackRefs() {
        var proc = new ReplaceText("(\\w+)@(\\w+)", "$2<-$1");
        var ff = FlowFile.create("alice@example".getBytes(), Map.of());
        var out = (ProcessorResult.Single) proc.process(ff);
        var content = (zincflow.core.RawContent) out.flowFile().content();
        assertEquals("example<-alice", new String(content.bytes()));
    }

    // --- SplitText ---

    @Test
    void splitTextFansOutMultiple() {
        var proc = new SplitText(",");
        var ff = FlowFile.create("a,b,c".getBytes(), Map.of());
        var multi = (ProcessorResult.Multiple) proc.process(ff);
        assertEquals(3, multi.flowFiles().size());
        var bytes = new String(((zincflow.core.RawContent) multi.flowFiles().get(1).content()).bytes());
        assertEquals("b", bytes);
        assertEquals("1", multi.flowFiles().get(1).attributes().get("split.index"));
        assertEquals(ff.stringId(), multi.flowFiles().get(1).attributes().get("split.parent"));
    }
}
