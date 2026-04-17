package caravanflow.fabric;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Serialises a running {@link PipelineGraph} + recorded
/// {@link ConfigLoader.ProcessorSpec} map back to YAML. Used by
/// {@code POST /api/flow/save} so the UI can commit graph edits
/// (processors added / removed / reconfigured through the admin
/// API) back to disk without the operator hand-editing YAML.
///
/// Uses SnakeYAML's {@link Yaml#dump(Object)} with block style and
/// a single-space indent, which is the idiomatic shape for
/// caravan-flow config files. Output keys are ordered
/// {@code flow → entryPoints, processors, connections} so
/// round-tripped files stay diff-stable against the committed base.
public final class YamlEmitter {

    private YamlEmitter() {}

    /// Emit a complete YAML document for the current graph + specs.
    public static String emit(PipelineGraph graph, Map<String, ConfigLoader.ProcessorSpec> specs) {
        if (graph == null) throw new IllegalArgumentException("graph must not be null");
        if (specs == null) specs = Map.of();

        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("entryPoints", List.copyOf(graph.entryPoints()));

        // Processors — preserve declaration order from the current graph.
        Map<String, Object> procs = new LinkedHashMap<>();
        for (String name : graph.processors().keySet()) {
            ConfigLoader.ProcessorSpec spec = specs.get(name);
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put(ConfigLoader.TYPE_KEY, spec == null
                    ? graph.processors().get(name).getClass().getSimpleName()
                    : spec.type());
            if (spec != null && !spec.config().isEmpty()) {
                entry.put(ConfigLoader.CONFIG_KEY, new LinkedHashMap<>(spec.config()));
            }
            procs.put(name, entry);
        }
        flow.put("processors", procs);

        // Connections — only include processors with non-empty outbound.
        if (!graph.connections().isEmpty()) {
            Map<String, Object> connections = new LinkedHashMap<>();
            for (var entry : graph.connections().entrySet()) {
                Map<String, Object> rels = new LinkedHashMap<>();
                for (var rel : entry.getValue().entrySet()) {
                    rels.put(rel.getKey(), List.copyOf(rel.getValue()));
                }
                if (!rels.isEmpty()) connections.put(entry.getKey(), rels);
            }
            if (!connections.isEmpty()) flow.put("connections", connections);
        }

        Map<String, Object> top = new LinkedHashMap<>();
        top.put("flow", flow);

        return dumper().dump(top);
    }

    private static Yaml dumper() {
        DumperOptions opts = new DumperOptions();
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        opts.setIndent(2);
        opts.setIndicatorIndent(0);
        opts.setPrettyFlow(true);
        return new Yaml(opts);
    }
}
