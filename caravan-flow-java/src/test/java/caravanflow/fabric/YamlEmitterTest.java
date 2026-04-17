package caravanflow.fabric;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class YamlEmitterTest {

    private static final String DEMO_YAML = """
            flow:
              entryPoints: [ingress]
              processors:
                ingress:
                  type: LogAttribute
                  config:
                    prefix: "[in] "
                router:
                  type: RouteOnAttribute
                  config:
                    routes: "high: priority == urgent"
              connections:
                ingress:
                  success: [router]
            """;

    @Test
    void roundTripPreservesGraphShape() {
        var loader = new ConfigLoader(new Registry());
        var graph = loader.load(DEMO_YAML);
        String emitted = YamlEmitter.emit(graph, loader.lastSpecs());

        // Parse it back and confirm structure matches.
        var loader2 = new ConfigLoader(new Registry());
        var reparsed = loader2.load(emitted);
        assertEquals(graph.processors().keySet(), reparsed.processors().keySet());
        assertEquals(graph.entryPoints(), reparsed.entryPoints());
        assertEquals(graph.connections(), reparsed.connections());

        // Processor specs survive untouched.
        for (String name : loader.lastSpecs().keySet()) {
            assertEquals(loader.lastSpecs().get(name), loader2.lastSpecs().get(name),
                    "spec for '" + name + "' changed on re-parse");
        }
    }

    @Test
    void doubleRoundTripIsStable() {
        var loader = new ConfigLoader(new Registry());
        loader.load(DEMO_YAML);
        String first = YamlEmitter.emit(loader.load(DEMO_YAML), loader.lastSpecs());
        String second = YamlEmitter.emit(loader.load(first), loader.lastSpecs());
        assertEquals(first, second,
                "two consecutive round-trips should produce byte-identical output");
    }

    @Test
    void processorWithEmptyConfigOmitsConfigKey() {
        var loader = new ConfigLoader(new Registry());
        var graph = loader.load("""
                flow:
                  entryPoints: [p]
                  processors:
                    p:
                      type: RouteOnAttribute
                """);
        String emitted = YamlEmitter.emit(graph, loader.lastSpecs());
        assertTrue(emitted.contains("type: RouteOnAttribute"));
        // No "config: {}" noise.
        assertFalse(emitted.contains("config: {}"), emitted);
    }

    @Test
    void connectionsSectionOmittedWhenEmpty() {
        var loader = new ConfigLoader(new Registry());
        var graph = loader.load("""
                flow:
                  entryPoints: [p]
                  processors:
                    p:
                      type: RouteOnAttribute
                """);
        String emitted = YamlEmitter.emit(graph, loader.lastSpecs());
        assertFalse(emitted.contains("connections:"), emitted);
    }

    @Test
    void specOrderingFollowsGraphInsertionOrder() {
        // Graph uses LinkedHashMap so insertion order is meaningful.
        var loader = new ConfigLoader(new Registry());
        var graph = loader.load("""
                flow:
                  entryPoints: [a]
                  processors:
                    a:
                      type: LogAttribute
                    c:
                      type: LogAttribute
                    b:
                      type: LogAttribute
                """);
        String emitted = YamlEmitter.emit(graph, loader.lastSpecs());
        int ai = emitted.indexOf("a:"), ci = emitted.indexOf("c:"), bi = emitted.indexOf("b:");
        assertTrue(ai < ci && ci < bi,
                "processor order should be a, c, b to match declaration; got:\n" + emitted);
    }
}
