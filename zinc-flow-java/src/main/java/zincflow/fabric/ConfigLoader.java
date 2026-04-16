package zincflow.fabric;

import org.yaml.snakeyaml.Yaml;
import zincflow.core.Processor;
import zincflow.core.ProcessorContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Builds a {@link PipelineGraph} from a YAML config file.
///
/// Expected shape (mirrors zinc-flow-csharp):
///
/// <pre>
/// flow:
///   entryPoints: [ingress]
///   processors:
///     ingress:
///       type: LogAttribute
///       config:
///         prefix: "[in] "
///     router:
///       type: RouteOnAttribute
///       config:
///         routes: "high: priority == urgent"
///   connections:
///     ingress:
///       success: [router]
///     router:
///       high: [sink]
///       unmatched: [sink]
/// </pre>
public final class ConfigLoader {

    private final Registry registry;
    private final ProcessorContext context;

    public ConfigLoader(Registry registry) {
        this(registry, new ProcessorContext());
    }

    public ConfigLoader(Registry registry, ProcessorContext context) {
        this.registry = registry;
        this.context = context == null ? new ProcessorContext() : context;
    }

    public ProcessorContext context() { return context; }

    public PipelineGraph loadFromFile(Path path) throws IOException {
        String yaml = Files.readString(path);
        return load(yaml);
    }

    @SuppressWarnings("unchecked")
    public PipelineGraph load(String yamlSource) {
        Object parsed = new Yaml().load(yamlSource);
        if (!(parsed instanceof Map<?, ?> topRaw)) {
            throw new IllegalArgumentException("config: top-level must be a map");
        }
        Object flowRaw = topRaw.get("flow");
        if (!(flowRaw instanceof Map<?, ?> flow)) {
            throw new IllegalArgumentException("config: missing 'flow' section");
        }

        // --- processors ---
        Object procsRaw = flow.get("processors");
        if (!(procsRaw instanceof Map<?, ?> procs)) {
            throw new IllegalArgumentException("config: 'flow.processors' must be a map");
        }
        Map<String, Processor> processors = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : procs.entrySet()) {
            String name = String.valueOf(entry.getKey());
            if (!(entry.getValue() instanceof Map<?, ?> def)) {
                throw new IllegalArgumentException("config: processor '" + name + "' must be a map");
            }
            Object type = def.get("type");
            if (type == null) {
                throw new IllegalArgumentException("config: processor '" + name + "' missing 'type'");
            }
            Map<String, String> config = stringMap(def.get("config"));
            Processor p = registry.create(String.valueOf(type), config, context);
            processors.put(name, p);
        }

        // --- connections ---
        Map<String, Map<String, List<String>>> connections = new HashMap<>();
        Object connsRaw = flow.get("connections");
        if (connsRaw instanceof Map<?, ?> conns) {
            for (Map.Entry<?, ?> fromEntry : conns.entrySet()) {
                String from = String.valueOf(fromEntry.getKey());
                if (!(fromEntry.getValue() instanceof Map<?, ?> rels)) {
                    throw new IllegalArgumentException(
                            "config: connections['" + from + "'] must be a map of relationship → targets");
                }
                Map<String, List<String>> relMap = new HashMap<>();
                for (Map.Entry<?, ?> relEntry : rels.entrySet()) {
                    String rel = String.valueOf(relEntry.getKey());
                    List<String> targets = stringList(relEntry.getValue());
                    relMap.put(rel, targets);
                }
                connections.put(from, relMap);
            }
        }

        // --- entry points ---
        List<String> entryPoints = stringList(flow.get("entryPoints"));
        if (entryPoints.isEmpty()) {
            throw new IllegalArgumentException("config: 'flow.entryPoints' must be a non-empty list");
        }
        // Validate that all referenced processors exist — catches typos at load
        // time rather than at first ingest.
        for (String ep : entryPoints) {
            if (!processors.containsKey(ep)) {
                throw new IllegalArgumentException("config: entryPoint '" + ep + "' is not defined in processors");
            }
        }
        for (var connEntry : connections.entrySet()) {
            String from = connEntry.getKey();
            if (!processors.containsKey(from)) {
                throw new IllegalArgumentException("config: connection source '" + from + "' is not defined");
            }
            for (List<String> targets : connEntry.getValue().values()) {
                for (String t : targets) {
                    if (!processors.containsKey(t)) {
                        throw new IllegalArgumentException(
                                "config: connection target '" + t + "' (from '" + from + "') is not defined");
                    }
                }
            }
        }

        return new PipelineGraph(processors, connections, entryPoints);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> stringMap(Object raw) {
        if (raw == null) return Map.of();
        if (!(raw instanceof Map<?, ?> m)) {
            throw new IllegalArgumentException("config: expected a map, got " + raw.getClass().getSimpleName());
        }
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<?, ?> entry : m.entrySet()) {
            out.put(String.valueOf(entry.getKey()),
                    entry.getValue() == null ? "" : String.valueOf(entry.getValue()));
        }
        return out;
    }

    private static List<String> stringList(Object raw) {
        if (raw == null) return List.of();
        if (!(raw instanceof List<?> list)) {
            throw new IllegalArgumentException("config: expected a list, got " + raw.getClass().getSimpleName());
        }
        List<String> out = new ArrayList<>(list.size());
        for (Object o : list) out.add(String.valueOf(o));
        return out;
    }
}
