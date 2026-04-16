package zincflow.fabric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import zincflow.core.Processor;
import zincflow.core.ProcessorContext;
import zincflow.core.Source;
import zincflow.sources.GenerateFlowFile;
import zincflow.sources.GetFile;

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

    private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);

    /// Recorded shape of a processor definition (type + config) from the
    /// last successful load. Keyed by processor name; consulted on the
    /// next load to decide which instances can be reused.
    public record ProcessorSpec(String type, Map<String, String> config) {
        public ProcessorSpec {
            config = Map.copyOf(config);
        }
    }

    private final Registry registry;
    private final ProcessorContext context;
    private Map<String, ProcessorSpec> lastSpecs = Map.of();
    private Map<String, Processor> lastProcessors = Map.of();
    private ConfigOverlay.Resolved lastOverlay;
    private List<Source> lastSources = List.of();

    public ConfigLoader(Registry registry) {
        this(registry, new ProcessorContext());
    }

    public ConfigLoader(Registry registry, ProcessorContext context) {
        this.registry = registry;
        this.context = context == null ? new ProcessorContext() : context;
    }

    public ProcessorContext context() { return context; }

    /// Snapshot of the most recently loaded processor spec map. Callers
    /// use this to diff the next load against the one that's currently
    /// running — see {@link Pipeline#applyReload}.
    public Map<String, ProcessorSpec> lastSpecs() { return lastSpecs; }

    /// The most recently loaded overlay stack — base + local + secrets,
    /// the merged map, and per-key provenance. Used by
    /// {@code GET /api/overlays}.
    public ConfigOverlay.Resolved lastOverlay() { return lastOverlay; }

    /// Sources constructed from the {@code sources:} block on the most
    /// recent load. Callers wire these into the pipeline via
    /// {@link Pipeline#addSource(Source)}; the loader does not start
    /// them — that's {@link Pipeline#startSource(String)}'s job.
    public List<Source> lastSources() { return lastSources; }

    public PipelineGraph loadFromFile(Path path) throws IOException {
        ConfigOverlay.Resolved resolved = ConfigOverlay.load(path);
        return loadFromOverlay(resolved);
    }

    public PipelineGraph loadFromOverlay(ConfigOverlay.Resolved resolved) {
        lastOverlay = resolved;
        return load(resolved.effective());
    }

    public PipelineGraph load(String yamlSource) {
        Object parsed = new Yaml().load(yamlSource);
        if (!(parsed instanceof Map<?, ?> topRaw)) {
            throw new IllegalArgumentException("config: top-level must be a map");
        }
        return load(normalizeTop(topRaw));
    }

    @SuppressWarnings("unchecked")
    private PipelineGraph load(Map<String, Object> effective) {
        Map<String, Object> top = effective;
        Object flowRaw = top.get("flow");
        if (!(flowRaw instanceof Map<?, ?> flow)) {
            throw new IllegalArgumentException("config: missing 'flow' section");
        }

        // --- processors ---
        Object procsRaw = flow.get("processors");
        if (!(procsRaw instanceof Map<?, ?> procs)) {
            throw new IllegalArgumentException("config: 'flow.processors' must be a map");
        }
        Map<String, Processor> processors = new LinkedHashMap<>();
        Map<String, ProcessorSpec> specs = new LinkedHashMap<>();
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
            ProcessorSpec spec = new ProcessorSpec(String.valueOf(type), config);

            // Reuse the prior processor instance when the spec is
            // byte-identical — keeps in-flight state (counters, caches,
            // connections) across a reload instead of churning every
            // processor on a cosmetic config change.
            Processor p;
            ProcessorSpec prior = lastSpecs.get(name);
            if (prior != null && prior.equals(spec) && lastProcessors.containsKey(name)) {
                p = lastProcessors.get(name);
            } else {
                p = registry.create(spec.type(), spec.config(), context);
            }
            processors.put(name, p);
            specs.put(name, spec);
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
        }

        // Full DAG check — accumulates every unknown-target error plus
        // cycle / unreachable warnings into one report. Errors trip a
        // single aggregate throw so the operator sees every issue at
        // once; warnings surface through the logger.
        FlowValidator.Result validation = FlowValidator.validate(processors.keySet(), connections);
        if (!validation.errors().isEmpty()) {
            throw new IllegalArgumentException(
                    "config: flow validation failed with " + validation.errors().size() + " error(s):\n"
                            + String.join("\n", validation.errors()));
        }
        for (String warn : validation.warnings()) {
            log.warn("flow warning: {}", warn);
        }

        // Commit the parsed spec + processor instances so the next
        // load can diff against them. Only happens after validation
        // succeeds — a partial load never corrupts the reload baseline.
        // Use an ordered unmodifiable view so spec iteration follows
        // declaration order (YAML round-trip relies on this).
        lastSpecs = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(specs));
        lastProcessors = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(processors));
        lastSources = buildSources(top.get("sources"));
        return new PipelineGraph(processors, connections, entryPoints);
    }

    private static List<Source> buildSources(Object sourcesRaw) {
        if (!(sourcesRaw instanceof Map<?, ?> m)) return List.of();
        List<Source> out = new ArrayList<>();
        Object file = m.get("file");
        if (file instanceof Map<?, ?> fileMap) {
            Source s = buildFileSource(stringKeyed(fileMap));
            if (s != null) out.add(s);
        }
        Object generate = m.get("generate");
        if (generate instanceof Map<?, ?> genMap) {
            Source s = buildGenerateSource(stringKeyed(genMap));
            if (s != null) out.add(s);
        }
        return List.copyOf(out);
    }

    private static Source buildFileSource(Map<String, Object> cfg) {
        String inputDir = str(cfg.get("input_dir"));
        if (inputDir.isEmpty()) {
            log.info("sources.file: input_dir missing — GetFile source disabled");
            return null;
        }
        String pattern = str(cfg.getOrDefault("pattern", "*"));
        long pollMs = longOr(cfg.get("poll_interval_ms"), 1000);
        boolean unpackV3 = boolOr(cfg.get("unpack_v3"), true);
        return new GetFile("file", Path.of(inputDir), pattern, pollMs, unpackV3);
    }

    private static Source buildGenerateSource(Map<String, Object> cfg) {
        String content = str(cfg.get("content"));
        // Matches C# behaviour: empty content disables the generator.
        // Keeps the config hospitable — you can leave the block
        // present (for env-overlay shape) without emitting anything.
        if (content.isEmpty()) {
            log.info("sources.generate: content empty — GenerateFlowFile disabled");
            return null;
        }
        String contentType = str(cfg.get("content_type"));
        String attributes = str(cfg.get("attributes"));
        int batchSize = (int) longOr(cfg.get("batch_size"), 1);
        long pollMs = longOr(cfg.get("poll_interval_ms"), 1000);
        return new GenerateFlowFile("generate", pollMs, content, contentType, attributes, batchSize);
    }

    private static String str(Object o) { return o == null ? "" : String.valueOf(o); }

    private static long longOr(Object o, long fallback) {
        if (o == null) return fallback;
        if (o instanceof Number n) return n.longValue();
        try { return Long.parseLong(o.toString().trim()); }
        catch (NumberFormatException ex) { return fallback; }
    }

    private static boolean boolOr(Object o, boolean fallback) {
        if (o == null) return fallback;
        if (o instanceof Boolean b) return b;
        return "true".equalsIgnoreCase(o.toString().trim());
    }

    private static Map<String, Object> stringKeyed(Map<?, ?> raw) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (var e : raw.entrySet()) out.put(String.valueOf(e.getKey()), e.getValue());
        return out;
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

    /// Normalises top-level keys to String. SnakeYAML default loader
    /// can yield {@code Map<Object, Object>}; downstream code expects
    /// {@code Map<String, Object>}.
    @SuppressWarnings("unchecked")
    private static Map<String, Object> normalizeTop(Map<?, ?> raw) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : raw.entrySet()) {
            out.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return out;
    }
}
