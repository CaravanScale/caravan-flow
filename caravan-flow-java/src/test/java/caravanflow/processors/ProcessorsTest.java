package caravanflow.processors;

import org.junit.jupiter.api.Test;
import caravanflow.core.FlowFile;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RawContent;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class ProcessorsTest {

    private static FlowFile singleOut(ProcessorResult r) {
        return switch (r) {
            case ProcessorResult.Single(FlowFile ff) -> ff;
            default -> {
                fail("expected Single, got " + r);
                yield null;
            }
        };
    }

    private static String routeOf(ProcessorResult r) {
        return switch (r) {
            case ProcessorResult.Routed(String name, FlowFile ignored) -> name;
            default -> {
                fail("expected Routed, got " + r);
                yield "";
            }
        };
    }

    private static List<FlowFile> multiOut(ProcessorResult r) {
        return switch (r) {
            case ProcessorResult.Multiple(List<FlowFile> ffs) -> ffs;
            default -> {
                fail("expected Multiple, got " + r);
                yield List.of();
            }
        };
    }

    // --- UpdateAttribute ---

    @Test
    void updateAttributeSetsKeyAndWrapsInSingle() {
        var ff = FlowFile.create(new byte[0], Map.of());
        var out = singleOut(new UpdateAttribute("priority", "high").process(ff));
        assertEquals("high", out.attributes().get("priority"));
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
        var out = singleOut(new LogAttribute("[t]").process(ff));
        assertSame(ff, out, "LogAttribute must be a pass-through — same FlowFile reference");
    }

    // --- RouteOnAttribute ---

    @Test
    void routeOnAttributeMatchesFirstRule() {
        var proc = new RouteOnAttribute("high: priority == urgent; low: priority == normal");
        var ff = FlowFile.create(new byte[0], Map.of("priority", "urgent"));
        assertEquals("high", routeOf(proc.process(ff)));
    }

    @Test
    void routeOnAttributeMatchesSecondRule() {
        var proc = new RouteOnAttribute("high: priority == urgent; low: priority == normal");
        var ff = FlowFile.create(new byte[0], Map.of("priority", "normal"));
        assertEquals("low", routeOf(proc.process(ff)));
    }

    @Test
    void routeOnAttributeFallsBackToUnmatched() {
        var proc = new RouteOnAttribute("high: priority == urgent");
        var ff = FlowFile.create(new byte[0], Map.of("priority", "bogus"));
        assertEquals("unmatched", routeOf(proc.process(ff)));
    }

    @Test
    void routeOnAttributeNeqOperator() {
        var proc = new RouteOnAttribute("errors: status != ok");
        var okFf = FlowFile.create(new byte[0], Map.of("status", "ok"));
        var badFf = FlowFile.create(new byte[0], Map.of("status", "fail"));
        assertEquals("unmatched", routeOf(proc.process(okFf)));
        assertEquals("errors", routeOf(proc.process(badFf)));
    }

    @Test
    void routeOnAttributeBlankSpecYieldsUnmatched() {
        var proc = new RouteOnAttribute("");
        var ff = FlowFile.create(new byte[0], Map.of("x", "y"));
        assertEquals("unmatched", routeOf(proc.process(ff)));
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
        var out = singleOut(proc.process(ff));
        if (out.content() instanceof RawContent(byte[] bytes)) {
            assertEquals("hello Java", new String(bytes));
        } else {
            fail("expected RawContent, got " + out.content());
        }
    }

    @Test
    void replaceTextSupportsRegexBackRefs() {
        var proc = new ReplaceText("(\\w+)@(\\w+)", "$2<-$1");
        var ff = FlowFile.create("alice@example".getBytes(), Map.of());
        var out = singleOut(proc.process(ff));
        if (out.content() instanceof RawContent(byte[] bytes)) {
            assertEquals("example<-alice", new String(bytes));
        } else {
            fail("expected RawContent, got " + out.content());
        }
    }

    // --- SplitText ---

    @Test
    void splitTextFansOutMultiple() {
        var proc = new SplitText(",");
        var ff = FlowFile.create("a,b,c".getBytes(), Map.of());
        var ffs = multiOut(proc.process(ff));
        assertEquals(3, ffs.size());
        if (ffs.get(1).content() instanceof RawContent(byte[] bytes)) {
            assertEquals("b", new String(bytes));
        } else {
            fail("expected RawContent at index 1");
        }
        assertEquals("1", ffs.get(1).attributes().get("split.index"));
        assertEquals(ff.stringId(), ffs.get(1).attributes().get("split.parent"));
    }
}
